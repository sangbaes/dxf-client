#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DXF Minimal Client (Streamlit)
- Upload DXF to Google Drive INBOX
- Create job in Firebase RTDB (queued)
- Poll job status from RTDB
- Download translated DXF from Google Drive OUTBOX when done

Secrets required (Streamlit Cloud -> Settings -> Secrets):
[gcp_service_account]
... (service account json fields)

[drive]
DXF_INBOX_FOLDER_ID="..."
DXF_OUTBOX_FOLDER_ID="..."

[rtdb]
url="https://<YOUR_DB>.firebaseio.com"
"""

import io
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

import streamlit as st

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

import firebase_admin
from firebase_admin import credentials, db


# -----------------------------
# Config
# -----------------------------
APP_TITLE = "DXF Client (Minimal)"
SCOPES = ["https://www.googleapis.com/auth/drive"]
MAX_FILE_MB = 200
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

SEOUL_TZ = timezone(timedelta(hours=9))
JOBS_PATH = "jobs"  # RTDB path root


# -----------------------------
# Time / Job helpers
# -----------------------------
def now_seoul_iso() -> str:
    return datetime.now(SEOUL_TZ).isoformat(timespec="seconds")


def make_job_id(original_name: str) -> str:
    ts = datetime.now(SEOUL_TZ).strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:8]
    base = (original_name or "file").replace(" ", "")
    safe = "".join(c for c in base if c.isascii() and (c.isalnum() or c in ("-", "_", ".")))
    safe = safe[:50] if safe else "file.dxf"
    return f"{ts}_{short}_{safe}"


# -----------------------------
# Secrets / Clients
# -----------------------------
def _get_sa_info() -> Dict[str, Any]:
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Missing [gcp_service_account] in Streamlit Secrets.")
    info = dict(st.secrets["gcp_service_account"])
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n").strip()
    return info


def _get_drive_folder_ids() -> Dict[str, str]:
    if "drive" not in st.secrets:
        raise RuntimeError("Missing [drive] in Streamlit Secrets.")
    inbox = st.secrets["drive"].get("DXF_INBOX_FOLDER_ID", "").strip()
    outbox = st.secrets["drive"].get("DXF_OUTBOX_FOLDER_ID", "").strip()
    if not inbox or not outbox:
        raise RuntimeError("Missing drive folder IDs. Need DXF_INBOX_FOLDER_ID and DXF_OUTBOX_FOLDER_ID.")
    return {"INBOX": inbox, "OUTBOX": outbox}


@st.cache_resource(show_spinner=False)
def get_drive_service():
    info = _get_sa_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


@st.cache_resource(show_spinner=False)
def init_rtdb():
    if "rtdb" not in st.secrets or "url" not in st.secrets["rtdb"]:
        raise RuntimeError("Missing [rtdb].url in Streamlit Secrets.")
    if not firebase_admin._apps:
        info = _get_sa_info()
        cred = credentials.Certificate(info)
        firebase_admin.initialize_app(cred, {"databaseURL": st.secrets["rtdb"]["url"]})


def jobs_ref():
    init_rtdb()
    return db.reference(JOBS_PATH)


# -----------------------------
# Drive operations (minimal)
# -----------------------------
def drive_upload_bytes(
    drive,
    folder_id: str,
    filename: str,
    data: bytes,
    mime: str = "application/dxf",
) -> Dict[str, Any]:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    body = {"name": filename, "parents": [folder_id]}
    # NOTE: If this fails with "Service Accounts do not have storage quota",
    # your INBOX folder is not on a Shared Drive OR the SA can't upload there.
    return drive.files().create(body=body, media_body=media, fields="id,name,size,createdTime").execute()


def drive_download_bytes(drive, file_id: str) -> bytes:
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


# -----------------------------
# RTDB job ops (minimal)
# -----------------------------
def create_job(job_id: str, original_filename: str, inbox_file_id: str) -> None:
    payload = {
        "job_id": job_id,
        "status": "queued",  # queued -> working -> done|error
        "original_filename": original_filename,
        "inbox_file_id": inbox_file_id,
        "outbox_file_id": None,
        "created_at": now_seoul_iso(),
        "updated_at": now_seoul_iso(),
        "message": "",
        "progress": 0,
    }
    jobs_ref().child(job_id).set(payload)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return jobs_ref().child(job_id).get()


def list_jobs(limit: int = 30) -> List[Dict[str, Any]]:
    data = jobs_ref().get() or {}
    # data is dict {job_id: payload}
    jobs = list(data.values())
    # sort by created_at desc (string ISO, works OK)
    jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return jobs[:limit]


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title=APP_TITLE, layout="centered")
st.title(APP_TITLE)
st.caption("Drive: INBOX 업로드 / OUTBOX 다운로드만. 잡 상태는 RTDB만 사용합니다.")

# Load clients early to fail fast with clear errors
try:
    drive = get_drive_service()
    folders = _get_drive_folder_ids()
except Exception as e:
    st.error(f"Drive 초기화 실패: {e}")
    st.stop()

try:
    init_rtdb()
except Exception as e:
    st.error(f"RTDB 초기화 실패: {e}")
    st.stop()

# -----------------------------
# Upload section
# -----------------------------
st.subheader("1) Upload DXF → INBOX")
uploaded = st.file_uploader("DXF 파일 선택", type=["dxf"], accept_multiple_files=False)

if uploaded:
    size = len(uploaded.getvalue())
    if size > MAX_FILE_BYTES:
        st.error(f"파일이 너무 큽니다. 최대 {MAX_FILE_MB}MB까지 지원합니다.")
    else:
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("Upload & Create Job", type="primary", use_container_width=True):
                try:
                    raw = uploaded.getvalue()
                    job_id = make_job_id(uploaded.name)
                    # Use job_id as file name prefix to keep tracking easy
                    drive_filename = f"{job_id}.dxf"
                    created = drive_upload_bytes(drive, folders["INBOX"], drive_filename, raw, mime="application/dxf")
                    inbox_file_id = created["id"]
                    create_job(job_id, uploaded.name, inbox_file_id)
                    st.success("업로드 완료 + RTDB 잡 생성 완료")
                    st.code(f"job_id: {job_id}\ninbox_file_id: {inbox_file_id}")
                    st.session_state["last_job_id"] = job_id
                except HttpError as he:
                    st.error(f"Drive 업로드 실패: {he}")
                    st.info(
                        "만약 에러 메시지에 'Service Accounts do not have storage quota'가 포함되면,\n"
                        "현재 폴더가 Shared Drive가 아니거나 서비스계정 업로드가 허용되지 않는 구조입니다.\n"
                        "이 경우 Shared Drive로 옮기는 것이 정석 해결입니다."
                    )
                except Exception as e:
                    st.error(f"업로드/잡 생성 실패: {type(e).__name__}: {e}")

        with col2:
            last = st.session_state.get("last_job_id")
            st.write("최근 생성한 job_id")
            st.code(last or "(없음)")

st.divider()

# -----------------------------
# Jobs section
# -----------------------------
st.subheader("2) Jobs (RTDB)")

colA, colB = st.columns([1, 1])
with colA:
    selected_job_id = st.text_input(
        "조회할 job_id (비워두면 목록에서 선택)",
        value=st.session_state.get("last_job_id", ""),
        placeholder="20260118_123456_abcd1234_file.dxf",
    )
with colB:
    auto_refresh = st.checkbox("자동 새로고침(5초)", value=False)

# List recent jobs
jobs = []
try:
    jobs = list_jobs(limit=30)
except Exception as e:
    st.warning(f"잡 목록 조회 실패: {e}")

if jobs:
    # Make a simple selector
    options = ["(선택 안 함)"] + [j.get("job_id", "(no id)") for j in jobs]
    pick = st.selectbox("최근 잡 선택", options, index=0)
    if pick != "(선택 안 함)":
        selected_job_id = pick

# Fetch job
job = None
if selected_job_id:
    try:
        job = get_job(selected_job_id)
    except Exception as e:
        st.warning(f"잡 조회 실패: {e}")

if job:
    st.write("**Job Status**")
    st.json(job)

    status = (job.get("status") or "").lower()
    prog = int(job.get("progress") or 0)
    st.progress(min(max(prog, 0), 100))

    if status == "done":
        out_id = job.get("outbox_file_id")
        if out_id:
            if st.button("Download Result DXF", use_container_width=True):
                try:
                    data = drive_download_bytes(drive, out_id)
                    out_name = job.get("result_filename") or f"{selected_job_id}_translated.dxf"
                    st.download_button(
                        "Click to save",
                        data=data,
                        file_name=out_name,
                        mime="application/dxf",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.error(f"다운로드 실패: {type(e).__name__}: {e}")
        else:
            st.warning("status=done 이지만 outbox_file_id가 없습니다. 워커가 RTDB 업데이트를 확인해 주세요.")

    elif status == "error":
        st.error(job.get("message") or "워커 처리 중 에러가 발생했습니다.")
else:
    st.info("job_id를 입력하거나 목록에서 선택하세요.")

# Auto refresh loop (lightweight)
if auto_refresh:
    time.sleep(5)
    st.rerun()

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
        return datetime.fromisoformat(s)
    except Exception:
        return None


def list_worker_heartbeats(drive, meta_folder_id: str, ttl_sec: int = 30):
    """
    Returns:
      active_workers: list[dict] (updated within ttl_sec)
      last_seen: datetime (most recent heartbeat regardless of ttl), or None
    Notes:
      Uses local datetime module alias to avoid import-name collisions.
    """
    import datetime as _dt

    def _parse_iso_dt(s: str):
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return _dt.datetime.fromisoformat(s)
        except Exception:
            return None

    # META에서 __worker__*.json 찾기
    res = drive.files().list(
        q=f"'{meta_folder_id}' in parents and trashed=false and name contains '__worker__'",
        fields="files(id,name)",
        pageSize=200,
    ).execute()
    files = res.get("files", [])

    now = _dt.datetime.now(_dt.timezone.utc)

    active = []
    last_seen = None

    for f in files:
        try:
            hb = download_json(drive, f["id"])  # 기존 함수 재사용
            updated_at = hb.get("updated_at")
            hb_time = _parse_iso_dt(updated_at)
            if hb_time is None:
                continue
            if hb_time.tzinfo is None:
                hb_time = hb_time.replace(tzinfo=_dt.timezone.utc)

            # last_seen 갱신
            if last_seen is None or hb_time > last_seen:
                last_seen = hb_time

            age = (now - hb_time).total_seconds()
            if age <= ttl_sec:
                hb["_hb_time"] = hb_time  # 정렬/표시에 사용(내부용)
                active.append(hb)
        except Exception:
            continue

    # 최근 업데이트 우선 정렬
    active.sort(key=lambda x: x.get("_hb_time") or _dt.datetime.min.replace(tzinfo=_dt.timezone.utc), reverse=True)
    return active, last_seen


#     active = []
#     for f in files:
#         try:
#             meta = download_json(drive, f["id"])
#             if meta.get("type") != "worker_heartbeat":
#                 continue
#             updated_at = meta.get("updated_at")
#             if not updated_at:
#                 continue
#             # Python 3.9: fromisoformat handles '+09:00' offsets
#             dt = _dt.datetime.fromisoformat(updated_at)
#             if dt.tzinfo is None:
#                 dt = dt.replace(tzinfo=_dt.timezone.utc)
#             age = (now - dt.astimezone(_dt.timezone.utc)).total_seconds()
#             if age <= ttl_sec:
#                 active.append((age, meta))
#         except Exception:
#             continue

#     # sort by worker_id for stable ordering (not by age)
#     active.sort(key=lambda x: (x[1].get("worker_id") or "", x[0]))
#     return [m for _, m in active]


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
active_workers, last_seen = list_worker_heartbeats(drive, folders["META"], ttl_sec=30)

if not active_workers:
    st.sidebar.markdown("🔴 **작업워커 없음**")
    if last_seen is not None:
        try:
            last_seen_local = last_seen.astimezone()
        except Exception:
            last_seen_local = last_seen
        st.sidebar.caption(f"마지막 접속: {last_seen_local.strftime('%Y-%m-%d %H:%M:%S')}")
else:
    # 여러 워커를 최근 heartbeat 순으로 표시
    for idx, hb in enumerate(active_workers, start=1):
        stt = (hb.get("status") or "").lower()
        if stt == "busy":
            st.sidebar.markdown(f"🟡 **{idx}번워커 번역중**")
            cj = hb.get("current_job_id")
            if cj:
                st.sidebar.caption(f"job: {cj}")
        else:
            st.sidebar.markdown(f"🟢 **{idx}번워커 대기중**")
        # 필요하면 worker_id를 아래에 표시(너무 길면 숨김 가능)
        wid = hb.get("worker_id")
        if wid:
            st.sidebar.caption(wid)

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
    try:
        meta = read_json_file_by_name(drive, folders["META"], meta_name)
    except Exception as e:
        st.error(f"Failed to read META\n\n{type(e).__name__}: {e}")
        meta = None


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
