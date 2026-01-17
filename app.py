#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DXF Translation Client (Streamlit Cloud) - RTDB Version
======================================================
- Upload DXF to Google Drive INBOX/
- Create job in Firebase RTDB (/jobs/{job_id})
- Read job status/progress from RTDB (no Drive META)
- Download result from Google Drive DONE/
"""

import base64
import json
import time
import uuid
import random
import socket
import ssl
import re
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone, timedelta

import httplib2
import streamlit as st

# Firebase Admin
import firebase_admin
from firebase_admin import credentials, db

# Google Drive
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError


# =========================
# Config
# =========================
DXF_SHARED_FOLDER_ID = "1qhx_xTGdOusxhV0xN2df4Kc8JTfh3zTd"  # 공유폴더 ID
SUBFOLDERS = ["INBOX", "WORKING", "DONE"]  # META 제거!

MAX_FILE_MB = 200
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

SEOUL_TZ = timezone(timedelta(hours=9))
SCOPES = ["https://www.googleapis.com/auth/drive"]


# =========================
# Utils
# =========================
def now_seoul_iso() -> str:
    return datetime.now(SEOUL_TZ).isoformat(timespec="seconds")


def _safe_name(name: str) -> str:
    safe = Path(name).name
    safe = safe.replace(" ", "_")
    safe = "".join(c for c in safe if c.isascii() and (c.isalnum() or c in ("-", "_", ".")))
    return safe if safe else "file.dxf"


def make_job_id(original_name: str) -> str:
    ts = datetime.now(SEOUL_TZ).strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:8]
    base = original_name.replace(" ", "")
    safe = "".join(c for c in base if c.isascii() and (c.isalnum() or c in ("-", "_", ".")))
    safe = safe[:40] if safe else "file"
    return f"{ts}_{short}_{safe}"


def make_batch_id() -> str:
    ts = datetime.now(SEOUL_TZ).strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{uuid.uuid4().hex[:8]}"


# =========================
# Firebase (RTDB)
# =========================
@st.cache_resource(show_spinner=False)
def init_firebase_rtdb():
    """
    Initialize Firebase Admin SDK for RTDB using base64-encoded service account JSON
    stored in Streamlit Secrets.

    Required secrets:
      [firebase]
      database_url = "https://....firebaseio.com" or "...firebasedatabase.app"
      service_account_b64 = "base64(JSON)"
    """
    fb = st.secrets.get("firebase")
    if not fb:
        raise RuntimeError("Missing [firebase] section in Streamlit Secrets")

    database_url = fb.get("database_url")
    if not database_url:
        raise RuntimeError("Missing firebase.database_url")

    sa_b64 = fb.get("service_account_b64")
    if not sa_b64:
        raise RuntimeError("Missing firebase.service_account_b64")

    # Be tolerant to accidental whitespace/newlines in Secrets
    sa_b64_clean = "".join(str(sa_b64).split())

    try:
        decoded = base64.b64decode(sa_b64_clean).decode("utf-8")
        sa_dict = json.loads(decoded)
    except Exception as e:
        raise RuntimeError("firebase.service_account_b64 could not be decoded as valid JSON") from e

    if sa_dict.get("type") != "service_account":
        raise RuntimeError('Invalid service account JSON: "type" must be "service_account"')

    cred_obj = credentials.Certificate(sa_dict)

    # Avoid duplicate init on Streamlit reruns
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred_obj, {"databaseURL": database_url})

    return True


def jobs_ref():
    return db.reference("jobs")


def write_job(job_id: str, payload: dict):
    jobs_ref().child(job_id).set(payload)


def update_job(job_id: str, patch: dict):
    patch["updated_at"] = now_seoul_iso()
    jobs_ref().child(job_id).update(patch)


def read_job(job_id: str):
    return jobs_ref().child(job_id).get()


def list_recent_jobs(limit: int = 50):
    # created_at 기준 최신 N개. (created_at은 ISO 문자열이므로 정렬이 잘 되도록 YYYY-MM-DDTHH:MM:SS 형태 유지)
    res = jobs_ref().order_by_child("created_at").limit_to_last(limit).get()
    if not res:
        return []
    # res: {job_id: payload, ...}
    items = list(res.values())
    items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return items


# =========================
# Google Drive
# =========================
@st.cache_resource(show_spinner=False)
def get_drive_service():
    if "drive_oauth" not in st.secrets:
        raise RuntimeError("Missing [drive_oauth] in Streamlit Secrets")

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

    metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
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
    try:
        file_obj.seek(0)
    except Exception:
        pass

    media = MediaIoBaseUpload(file_obj, mimetype=mime, resumable=False)
    metadata = {"name": filename, "parents": [folder_id]}
    req = drive.files().create(body=metadata, media_body=media, fields="id,name,size,createdTime")
    return req.execute(num_retries=5)


def find_file_in_folder_by_name(drive, folder_id: str, filename: str):
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    res = drive.files().list(q=q, fields="files(id,name,size,modifiedTime)").execute(num_retries=3)
    files = res.get("files", [])
    return files[0] if files else None


def download_file_bytes(drive, file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id)
    buf = BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="DXF Client (RTDB)", layout="centered")
st.title("DXF Translation Client (RTDB)")

# Init Firebase + Drive
try:
    init_firebase()
    drive = get_drive_service()
    folders = get_subfolder_ids(drive)
except Exception as e:
    st.error("초기화 실패: Secrets / 권한 설정을 확인하세요.")
    st.exception(e)
    st.stop()

st.success("✅ Connected (Firebase RTDB + Google Drive)")

# Sidebar
st.sidebar.header("Options")
auto_refresh = st.sidebar.checkbox("Auto-refresh status", value=True)
refresh_sec = st.sidebar.slider("Refresh interval (sec)", 3, 30, 5)

if auto_refresh:
    try:
        from streamlit import st_autorefresh
        st_autorefresh(interval=refresh_sec * 1000, key="job_poll")
    except Exception:
        pass

# =========================
# 1) Upload -> Drive INBOX + RTDB job create
# =========================
st.subheader("1) Upload DXF (Drive INBOX + RTDB job)")

uploaded_list = st.file_uploader(
    "Select DXF files (multiple allowed)",
    type=["dxf", "DXF"],
    accept_multiple_files=True,
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
        if st.button("Batch Upload & Create Jobs", type="primary"):
            batch_id = make_batch_id()
            created_at = now_seoul_iso()

            prog = st.progress(0)
            status_box = st.empty()

            ok, fail = 0, 0
            created_job_ids = []

            for idx, uploaded in enumerate(uploaded_list, 1):
                try:
                    safe_orig = _safe_name(uploaded.name)
                    job_id = make_job_id(safe_orig)
                    inbox_name = f"{job_id}__{safe_orig}"

                    # 1) Drive upload
                    resp = upload_file_to_folder(
                        drive,
                        folders["INBOX"],
                        inbox_name,
                        uploaded,
                        mime=getattr(uploaded, "type", None) or "application/dxf",
                    )
                    inbox_file_id = resp.get("id")

                    # 2) RTDB job create
                    payload = {
                        "job_id": job_id,
                        "batch_id": batch_id,
                        "original_name": safe_orig,
                        "inbox_name": inbox_name,
                        "inbox_file_id": inbox_file_id,
                        "status": "queued",
                        "progress": 0,
                        "message": "Uploaded to INBOX. Waiting for worker.",
                        "error": None,
                        "worker_id": None,
                        "done_name": None,
                        "done_file_id": None,
                        "created_at": created_at,
                        "updated_at": created_at,
                    }
                    write_job(job_id, payload)

                    ok += 1
                    created_job_ids.append(job_id)
                except Exception as e:
                    fail += 1
                    st.warning(f"Upload/Create failed: {uploaded.name} ({type(e).__name__})")

                pct = int((idx / total_files) * 100)
                prog.progress(pct)
                status_box.write(f"{idx}/{total_files} (ok {ok} / failed {fail})")

            st.session_state["active_batch_id"] = batch_id
            st.session_state["active_job_ids"] = created_job_ids
            st.success(f"✅ Completed. batch_id={batch_id} (jobs: {ok}, failed: {fail})")


# =========================
# 2) Status (from RTDB) + Download (from Drive DONE)
# =========================
st.subheader("2) Job Status / Download (RTDB)")

left, right = st.columns([1, 1])
with left:
    st.button("Refresh now")  # rerun trigger (no-op)
with right:
    st.caption("Status is read from RTDB. Result is downloaded from Drive DONE.")

jobs = list_recent_jobs(limit=50)
if not jobs:
    st.info("No jobs yet. Upload files first.")
    st.stop()

job_ids = [j.get("job_id") for j in jobs if j.get("job_id")]
default_job = st.session_state.get("active_job_id") or (st.session_state.get("active_job_ids", [])[:1] or [None])[0]
if default_job in job_ids:
    idx = job_ids.index(default_job)
else:
    idx = 0

selected_job_id = st.selectbox("Select recent job", job_ids, index=idx)
st.session_state["active_job_id"] = selected_job_id

job = read_job(selected_job_id)
if not job:
    st.warning("Job not found in RTDB (maybe deleted).")
    st.stop()

st.write(f"**status:** `{job.get('status')}`")
st.write(f"**updated_at:** `{job.get('updated_at')}`")
st.write(f"**message:** {job.get('message')}")
if job.get("worker_id"):
    st.write(f"**worker_id:** `{job.get('worker_id')}`")

prog_val = int(job.get("progress", 0) or 0)
st.progress(min(max(prog_val, 0), 100) / 100.0)

if job.get("status") == "error":
    st.error("Job failed")
    if job.get("error"):
        st.code(str(job.get("error")))

if job.get("status") == "done":
    st.success("✅ Translation completed")

    done_file_id = job.get("done_file_id")
    done_name = job.get("done_name")

    # DONE 파일을 찾는 우선순위:
    # 1) done_file_id가 있으면 바로 다운로드
    # 2) 없으면 done_name으로 DONE 폴더에서 검색
    try:
        if done_file_id:
            data = download_file_bytes(drive, done_file_id)
            filename = done_name or f"{selected_job_id}__result.dxf"
            st.download_button(
                label="Download Result DXF",
                data=data,
                file_name=filename,
                mime="application/dxf",
                type="primary",
            )
        elif done_name:
            obj = find_file_in_folder_by_name(drive, folders["DONE"], done_name)
            if not obj:
                st.warning("Result file not found in DONE folder yet. Try again later.")
            else:
                data = download_file_bytes(drive, obj["id"])
                st.download_button(
                    label="Download Result DXF",
                    data=data,
                    file_name=done_name,
                    mime="application/dxf",
                    type="primary",
                )
        else:
            st.warning("RTDB job has status=done but (done_file_id/done_name) is missing.")
    except Exception as e:
        st.error("Failed to download result")
        st.exception(e)