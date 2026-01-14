#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DXF Translation Client (English Version)
========================================
Streamlit app for uploading DXF files and monitoring translation jobs
"""
import base64
import json
import time
import uuid
from pathlib import Path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from io import BytesIO
import re
from datetime import datetime, timezone, timedelta

import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import socket
import ssl
import httplib2
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.errors import HttpError


# =========================
# Config
# =========================
DXF_SHARED_FOLDER_ID = "1qhx_xTGdOusxhV0xN2df4Kc8JTfh3zTd"

SUBFOLDERS = ["INBOX", "WORKING", "DONE", "META"]
MAX_FILE_MB = 200
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

SEOUL_TZ = timezone(timedelta(hours=9))

SCOPES = ["https://www.googleapis.com/auth/drive"]


# =========================
# Helpers
# =========================
def now_seoul_iso() -> str:
    return datetime.now(SEOUL_TZ).isoformat(timespec="seconds")


def make_job_id(original_name: str) -> str:
    ts = datetime.now(SEOUL_TZ).strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:8]
    # Remove spaces and keep only safe ASCII characters for Drive filenames.
    # Some environments/HTTP stacks are surprisingly fragile with non-ASCII names.
    base = original_name.replace(" ", "")
    safe = "".join(c for c in base if c.isascii() and (c.isalnum() or c in ("-", "_", ".")))
    safe = safe[:40] if safe else "file"
    return f"{ts}_{short}_{safe}"


def drive_execute(req, retries: int = 5, base_sleep: float = 0.6):
    """Execute Drive API request with retry logic for network stability"""
    last_err = None
    for i in range(retries + 1):
        try:
            return req.execute(num_retries=1)
        except (HttpError, OSError, ssl.SSLError, socket.timeout) as e:
            last_err = e
            if i >= retries:
                raise
            time.sleep(base_sleep * (2 ** i))
    raise last_err


def load_service_account_info():
    # Base64 method (Streamlit Secrets: SERVICE_ACCOUNT_B64)
    if "SERVICE_ACCOUNT_B64" not in st.secrets:
        raise RuntimeError("SERVICE_ACCOUNT_B64 not found in Streamlit Secrets")

    raw = base64.b64decode(st.secrets["SERVICE_ACCOUNT_B64"].encode("ascii"))
    info = json.loads(raw.decode("utf-8"))

    # Fix escaped newlines in private_key
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n").strip()

    return info


SCOPES = ["https://www.googleapis.com/auth/drive"]


@st.cache_resource(show_spinner=False)
def get_drive_service():
    s = st.secrets["drive_oauth"]
    creds = Credentials(
        token=None,
        refresh_token=s["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=s["client_id"],
        client_secret=s["client_secret"],
        scopes=SCOPES,
    )
    creds.refresh(Request())

    # Use AuthorizedHttp with timeout for better stability on Streamlit Cloud
    authed_http = AuthorizedHttp(creds, http=httplib2.Http(timeout=30))
    return build("drive", "v3", http=authed_http, cache_discovery=False)


def find_or_create_folder(drive, parent_id: str, name: str) -> str:
    q = (
        f"'{parent_id}' in parents and "
        f"name = '{name}' and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    res = drive.files().list(q=q, fields="files(id,name)").execute(num_retries=3)
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = drive.files().create(body=metadata, fields="id").execute()
    return folder["id"]


def get_subfolder_ids(drive):
    if "subfolder_ids" in st.session_state:
        return st.session_state["subfolder_ids"]

    ids = {}
    for name in SUBFOLDERS:
        ids[name] = find_or_create_folder(drive, DXF_SHARED_FOLDER_ID, name)

    st.session_state["subfolder_ids"] = ids
    return ids


def upload_file_to_folder(drive, folder_id: str, filename: str, file_obj, mime: str):
    """Upload a file-like object to Drive.

    IMPORTANT: We intentionally default to *non-resumable* multipart upload.
    We've seen 'Redirected but the response is missing a Location: header.'
    when using resumable uploads in some environments. Multipart is more stable
    for files in our size range.
    """
    try:
        file_obj.seek(0)
    except Exception:
        pass

    media = MediaIoBaseUpload(file_obj, mimetype=mime, resumable=False)
    metadata = {"name": filename, "parents": [folder_id]}
    req = drive.files().create(body=metadata, media_body=media, fields="id,name,size,createdTime")
    # execute with retry/backoff
    return drive_execute(req, retries=6, base_sleep=0.8)


def upsert_json_file(drive, folder_id: str, filename: str, payload: dict):
    q = (
        f"'{folder_id}' in parents and "
        f"name = '{filename}' and "
        "mimeType != 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    res = drive.files().list(q=q, fields="files(id,name)").execute()
    files = res.get("files", [])

    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    media = MediaIoBaseUpload(BytesIO(data), mimetype="application/json", resumable=False)

    if files:
        file_id = files[0]["id"]
        updated = drive.files().update(fileId=file_id, media_body=media).execute()
        return updated
    else:
        meta = {"name": filename, "parents": [folder_id]}
        created = drive.files().create(body=meta, media_body=media, fields="id").execute()
        return created


def read_json_file_by_name(drive, folder_id: str, filename: str) -> dict | None:
    q = (
        f"'{folder_id}' in parents and "
        f"name = '{filename}' and "
        "trashed = false"
    )
    res = drive.files().list(q=q, fields="files(id,name)").execute()
    files = res.get("files", [])
    if not files:
        return None

    file_id = files[0]["id"]
    request = drive.files().get_media(fileId=file_id)

    buf = BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    buf.seek(0)
    return json.loads(buf.read().decode("utf-8"))


def list_recent_jobs(drive, meta_folder_id: str, limit: int = 20):
    q = f"'{meta_folder_id}' in parents and trashed=false"
    req = drive.files().list(
        q=q,
        fields="files(id,name,createdTime,modifiedTime,size)",
        orderBy="modifiedTime desc",
        pageSize=limit,
    )

    try:
        res = drive_execute(req, retries=5)
    except Exception as e:
        st.warning(
            f"Google Drive query temporarily failed. Will retry automatically.\n"
            f"Reason: {type(e).__name__}"
        )
        return []

    files = res.get("files", [])
    files = [f for f in files if f.get("name", "").lower().endswith(".json")]
    return files


def download_file_bytes(drive, file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id)
    buf = BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


def find_file_in_folder_by_name(drive, folder_id: str, filename: str):
    q = (
        f"'{folder_id}' in parents and "
        f"name = '{filename}' and "
        "trashed = false"
    )
    res = drive.files().list(q=q, fields="files(id,name,size,modifiedTime)").execute()
    files = res.get("files", [])
    return files[0] if files else None


def _safe_name(name: str) -> str:
    """
    Prevent weird path-ish names and remove spaces
    """
    safe = Path(name).name
    # Replace spaces with underscores
    safe = safe.replace(" ", "_")
    # Remove any remaining problematic characters (keep ASCII only for stability)
    safe = "".join(c for c in safe if c.isascii() and (c.isalnum() or c in ("-", "_", ".")))
    return safe if safe else "file.dxf"


def _make_batch_id() -> str:
    """Generate time-sortable batch ID"""
    ts = now_seoul_iso().replace(":", "").replace("-", "").replace("+0900", "").replace("T", "_")
    ts = ts.split(".")[0].replace("+09:00", "").replace("+0900", "")
    return f"{ts}_{uuid.uuid4().hex[:8]}"


# =========================


def _parse_iso_dt(s: str):
    """Parse ISO datetime from worker heartbeat. Accepts 'Z' suffix."""
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(s)
    except Exception:
        return None


def list_worker_heartbeats(drive, meta_folder_id: str, ttl_sec: int = 30, limit: int = 50):
    """Return active worker heartbeats from META folder."""
    q = f"'{meta_folder_id}' in parents and trashed=false and name contains '__worker__'"
    res = drive.files().list(
        q=q,
        fields="files(id,name,modifiedTime,createdTime,size)",
        orderBy="modifiedTime desc",
        pageSize=limit
    ).execute()
    files = res.get("files", [])
    now = datetime.datetime.now(datetime.timezone.utc)

    active = []
    for f in files:
        try:
            meta = download_json(drive, f["id"])
            if meta.get("type") != "worker_heartbeat":
                continue
            updated_at = meta.get("updated_at")
            if not updated_at:
                continue
            # Python 3.9: fromisoformat handles '+09:00' offsets
            dt = datetime.datetime.fromisoformat(updated_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            age = (now - dt.astimezone(datetime.timezone.utc)).total_seconds()
            if age <= ttl_sec:
                active.append((age, meta))
        except Exception:
            continue

    # sort by worker_id for stable ordering (not by age)
    active.sort(key=lambda x: (x[1].get("worker_id") or "", x[0]))
    return [m for _, m in active]

# UI
# =========================
st.set_page_config(page_title="DXF Client", layout="centered")

# Google Analytics
st.components.v1.html(
    """
    <!-- Google tag (gtag.js) -->
    <script async src="https://www.googletagmanager.com/gtag/js?id=G-E1LFDTNPVP"></script>
    <script>
      window.dataLayer = window.dataLayer || [];
      function gtag(){dataLayer.push(arguments);}
      gtag('js', new Date());
      gtag('config', 'G-E1LFDTNPVP');
    </script>
    """,
    height=0,
)

st.title("DXF Translation Client")

with st.expander("About", expanded=False):
    st.markdown(
        """
- This app uploads DXF files to a Google Drive shared folder
- A local worker translates them and uploads results to `DONE/`
- The app detects completion and provides a download button
        """
    )

# Drive connection
try:
    drive = get_drive_service()
    folders = get_subfolder_ids(drive)
except Exception as e:
    st.error("Failed to connect to Google Drive or initialize folders. Please check Secrets and folder sharing permissions.")
    st.exception(e)
    st.stop()

st.success("✅ Connected to Google Drive")

# Sidebar controls
st.sidebar.header("Options")
auto_refresh = st.sidebar.checkbox("Auto-refresh status", value=True)
refresh_sec = st.sidebar.slider("Refresh interval (sec)", 3, 30, 5)

st.sidebar.divider()
st.sidebar.subheader("Worker status")

active_workers = list_worker_heartbeats(drive, folders["META"], ttl_sec=30)

if not active_workers:
    st.sidebar.write("작업워커 없음")
else:
    for i, w in enumerate(active_workers, start=1):
        stt = w.get("status")
        if stt == "busy":
            st.sidebar.write(f"{i}번워커 번역중")
        else:
            st.sidebar.write(f"{i}번워커 대기중")

# Upload section
st.subheader("1) Upload DXF Files (Batch)")
uploaded_list = st.file_uploader(
    "Select DXF files (multiple allowed)",
    type=["dxf"],
    accept_multiple_files=True
)

if uploaded_list:
    total_files = len(uploaded_list)
    total_size = sum(u.size for u in uploaded_list)

    st.write(f"Selected files: **{total_files}** / Total size: **{total_size/1024/1024:.1f} MB**")
    too_big = [u for u in uploaded_list if u.size > MAX_FILE_BYTES]

    if too_big:
        st.error(
            f"The following files are too large. Maximum size is {MAX_FILE_MB}MB:\n- "
            + "\n- ".join([f"{u.name} ({u.size/1024/1024:.1f}MB)" for u in too_big])
        )
    else:
        # Batch upload button
        if st.button("Batch Upload to INBOX", type="primary"):
            st.session_state.pop("upload_progress", None)

            batch_id = _make_batch_id()
            created_at = now_seoul_iso()

            manifest_filename = f"{batch_id}__manifest.json"
            manifest_payload = {
                "batch_id": batch_id,
                "status": "uploading",
                "created_at": created_at,
                "updated_at": created_at,
                "total": total_files,
                "items": [],
                "message": "Uploading files to INBOX and writing META items."
            }

            progress = st.progress(0)
            status_box = st.empty()

            ok_count = 0
            errors = []

            with st.spinner("Uploading..."):
                for idx, uploaded in enumerate(uploaded_list, 1):
                    try:
                        safe_orig = _safe_name(uploaded.name)
                        # Streamlit's UploadedFile is a file-like object.
                        # Avoid .getvalue() to prevent large in-memory copies.
                        file_obj = uploaded
                        try:
                            file_obj.seek(0)
                        except Exception:
                            pass

                        job_id = make_job_id(safe_orig)

                        inbox_name = f"{job_id}__{safe_orig}"
                        meta_filename = f"{job_id}.json"

                        meta_payload = {
                            "batch_id": batch_id,
                            "job_id": job_id,
                            "original_name": safe_orig,
                            "inbox_name": inbox_name,
                            "status": "queued",
                            "created_at": created_at,
                            "updated_at": now_seoul_iso(),
                            "progress": 0,
                            "message": "Uploaded to INBOX. Waiting for local worker.",
                            "done_file": None,
                            "error": None,
                        }

                        # 1) Upload DXF to INBOX
                        resp = upload_file_to_folder(
                            drive,
                            folders["INBOX"],
                            inbox_name,
                            file_obj,
                            mime=getattr(uploaded, "type", None) or "application/dxf"
                        )
                        meta_payload["inbox_file_id"] = resp.get("id")
                        meta_payload["progress"] = 5
                        meta_payload["updated_at"] = now_seoul_iso()

                        # 2) Upsert META
                        upsert_json_file(drive, folders["META"], meta_filename, meta_payload)

                        # 3) Add to manifest
                        manifest_payload["items"].append({
                            "job_id": job_id,
                            "meta_filename": meta_filename,
                            "original_name": safe_orig,
                            "inbox_name": inbox_name,
                            "inbox_file_id": meta_payload.get("inbox_file_id"),
                            "status": "queued",
                        })

                        ok_count += 1

                    except Exception as e:
                        errors.append({"file": uploaded.name, "error": str(e)})

                    # UI progress update
                    pct = int((idx / total_files) * 100)
                    progress.progress(pct)
                    status_box.write(f"Upload progress: {idx}/{total_files} (success {ok_count} / failed {len(errors)})")

            # Finalize manifest
            manifest_payload["updated_at"] = now_seoul_iso()
            if errors:
                manifest_payload["status"] = "error"
                manifest_payload["message"] = f"Uploaded with errors: {len(errors)} failed."
                manifest_payload["errors"] = errors
            else:
                manifest_payload["status"] = "queued"
                manifest_payload["message"] = "All files uploaded. Waiting for local worker."

            # Write manifest
            try:
                upsert_json_file(drive, folders["META"], manifest_filename, manifest_payload)
            except Exception as e:
                st.error("❌ Failed to save manifest")
                st.exception(e)

            # Store batch context
            st.session_state["active_batch_id"] = batch_id
            st.session_state["active_job_ids"] = [it["job_id"] for it in manifest_payload["items"]]

            if errors:
                st.warning(f"⚠️ Some uploads failed: {len(errors)} files")
                st.json(errors)
            st.success("✅ Batch upload completed")
            st.code(f"batch_id: {batch_id}")


st.subheader("2) Job Status / Download")

# Load recent jobs
recent = list_recent_jobs(drive, folders["META"], limit=30)
recent_ids = [f["name"].replace(".json", "") for f in recent]

default_job = st.session_state.get("active_job_id")
if default_job and default_job in recent_ids:
    default_index = recent_ids.index(default_job)
else:
    default_index = 0 if recent_ids else None

job_id = None
if recent_ids:
    job_id = st.selectbox("Select recent job", recent_ids, index=default_index)
else:
    st.info("No jobs in META folder yet. Upload files first.")

# Auto refresh
def do_autorefresh():
    try:
        from streamlit import st_autorefresh
        st_autorefresh(interval=refresh_sec * 1000, key="job_poll")
    except Exception:
        pass

if auto_refresh:
    do_autorefresh()

col_a, col_b = st.columns([1, 1])
with col_a:
    manual_refresh = st.button("Refresh Status")
with col_b:
    st.caption("Use button if auto-refresh doesn't work")

if job_id:
    meta_name = f"{job_id}.json"
    meta = None
    try:
        meta = read_json_file_by_name(drive, folders["META"], meta_name)
    except Exception as e:
        st.error("Failed to read META")
        st.exception(e)

    if meta:
        st.write(f"**status:** `{meta.get('status')}`")
        st.write(f"**updated_at:** `{meta.get('updated_at')}`")
        st.write(f"**message:** {meta.get('message')}")
        prog = int(meta.get("progress", 0) or 0)
        st.progress(min(max(prog, 0), 100) / 100.0)

        if meta.get("status") == "error":
            st.error("Job failed")
            if meta.get("error"):
                st.code(meta.get("error"))

        if meta.get("status") == "done":
            done_file = meta.get("done_file")
            if not done_file:
                st.warning("Status is 'done' but done_file is missing in META")
            else:
                st.success("✅ Translation completed")
                st.write(f"Result file: `{done_file}`")

                done_obj = find_file_in_folder_by_name(drive, folders["DONE"], done_file)
                if not done_obj:
                    st.warning("Result file not found in DONE folder yet. Please try again later.")
                else:
                    try:
                        with st.spinner("Preparing download... (may take time for large files)"):
                            data = download_file_bytes(drive, done_obj["id"])
                        st.download_button(
                            label="Download Result DXF",
                            data=data,
                            file_name=done_file,
                            mime="application/dxf",
                            type="primary",
                        )
                    except Exception as e:
                        st.error("Failed to prepare download")
                        st.exception(e)

    else:
        st.info("META file not found for this job yet")
