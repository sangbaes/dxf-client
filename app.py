#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DXF Minimal Client (Streamlit)

What this client does (and ONLY does)
- Upload DXF to Google Drive INBOX
- Create a job record in Firebase Realtime Database (RTDB)
- Show job status from RTDB
- Download translated DXF when job.status == 'done' and job.outbox_file_id is present

What this client does NOT do
- No Drive folder auto-creation (no DXF_SHARED_FOLDER_ID, no get_subfolder_ids)
- No META/manifest JSON writes to Drive
- No worker heartbeat / worker list checks

Required Streamlit Secrets (Settings -> Secrets):

[gcp_service_account]
# Paste your service account JSON fields here as TOML keys.
# Make sure private_key keeps \n newlines (Streamlit secrets commonly escape them).

[drive]
DXF_INBOX_FOLDER_ID = "..."
DXF_OUTBOX_FOLDER_ID = "..."  # same as DONE/outbox folder

[rtdb]
url = "https://<YOUR_DB>.asia-southeast1.firebasedatabase.app"

Notes
- If Drive upload fails with: "Service Accounts do not have storage quota",
  your target folder is not on a Shared Drive (Workspace) or the SA cannot upload there.
  In that case, the correct fix is to use a Shared Drive.
"""

from __future__ import annotations

import io
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import streamlit as st

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

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
JOBS_PATH = "jobs"  # RTDB root path


# -----------------------------
# Small helpers
# -----------------------------
def now_seoul_iso() -> str:
    return datetime.now(SEOUL_TZ).isoformat(timespec="seconds")


def make_job_id(original_name: str) -> str:
    ts = datetime.now(SEOUL_TZ).strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:8]
    base = (original_name or "file.dxf").replace(" ", "")
    safe = "".join(c for c in base if c.isascii() and (c.isalnum() or c in ("-", "_", ".")))
    safe = safe[:60] if safe else "file.dxf"
    if not safe.lower().endswith(".dxf"):
        safe += ".dxf"
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
    inbox = (st.secrets["drive"].get("DXF_INBOX_FOLDER_ID") or "").strip()
    outbox = (st.secrets["drive"].get("DXF_OUTBOX_FOLDER_ID") or "").strip()
    if not inbox or not outbox:
        raise RuntimeError("Need drive.DXF_INBOX_FOLDER_ID and drive.DXF_OUTBOX_FOLDER_ID in Secrets.")
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

    # Avoid double-init on Streamlit reruns
    if not firebase_admin._apps:
        info = _get_sa_info()
        cred = credentials.Certificate(info)
        firebase_admin.initialize_app(cred, {"databaseURL": st.secrets["rtdb"]["url"].rstrip("/")})


def jobs_ref():
    init_rtdb()
    return db.reference(JOBS_PATH)


# -----------------------------
# Drive operations (minimal)
# -----------------------------
def drive_upload_bytes(drive, folder_id: str, filename: str, data: bytes) -> Dict[str, Any]:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/dxf", resumable=False)
    body = {"name": filename, "parents": [folder_id]}
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
        "status": "queued",  # queued -> working -> done | error
        "original_filename": original_filename,
        "inbox_file_id": inbox_file_id,
        "outbox_file_id": None,
        "result_filename": None,
        "progress": 0,
        "message": "",
        "created_at": now_seoul_iso(),
        "updated_at": now_seoul_iso(),
    }
    jobs_ref().child(job_id).set(payload)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return jobs_ref().child(job_id).get()


def list_jobs(limit: int = 30) -> List[Dict[str, Any]]:
    data = jobs_ref().get() or {}
    jobs = list(data.values())
    jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return jobs[:limit]


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title=APP_TITLE, layout="centered")
st.title(APP_TITLE)
st.caption("Drive: INBOX 업로드 / OUTBOX 다운로드만. Job 상태는 RTDB만 사용합니다.")

# Init clients (fail fast with clear errors)
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

# ---- Upload ----
st.subheader("1) Upload DXF → INBOX")
file = st.file_uploader("DXF 파일 선택", type=["dxf"], accept_multiple_files=False)

if file is not None:
    raw = file.getvalue()
    if len(raw) > MAX_FILE_BYTES:
        st.error(f"파일이 너무 큽니다. 최대 {MAX_FILE_MB}MB까지 지원합니다.")
    else:
        if st.button("Upload & Create Job", type="primary", use_container_width=True):
            job_id = make_job_id(file.name)
            drive_filename = f"{job_id}.dxf"
            try:
                created = drive_upload_bytes(drive, folders["INBOX"], drive_filename, raw)
                inbox_file_id = created["id"]
                create_job(job_id, file.name, inbox_file_id)
                st.success("업로드 완료 + RTDB 잡 생성 완료")
                st.session_state["last_job_id"] = job_id
                st.code(f"job_id: {job_id}\ninbox_file_id: {inbox_file_id}")
            except HttpError as he:
                st.error(f"Drive 업로드 실패: {he}")
                st.info(
                    "에러에 'Service Accounts do not have storage quota'가 포함되면,\n"
                    "현재 폴더가 Shared Drive가 아니거나 서비스계정 업로드가 가능한 구조가 아닙니다.\n"
                    "정석 해결: Shared Drive(Workspace)로 INBOX/OUTBOX를 옮기고 서비스계정을 멤버로 추가."
                )
            except Exception as e:
                st.error(f"업로드/잡 생성 실패: {type(e).__name__}: {e}")

st.divider()

# ---- Jobs ----
st.subheader("2) Jobs (RTDB)")

colA, colB = st.columns([2, 1])
with colA:
    job_id_input = st.text_input(
        "job_id 입력 (비워두면 아래 목록에서 선택)",
        value=st.session_state.get("last_job_id", ""),
        placeholder="20260118_123456_abcd1234_file.dxf",
    )
with colB:
    auto_refresh = st.checkbox("자동 새로고침(5초)", value=False)

# Recent jobs selector
try:
    recent = list_jobs(limit=30)
except Exception as e:
    recent = []
    st.warning(f"잡 목록 조회 실패: {e}")

selected_job_id = job_id_input.strip()
if recent:
    opts = ["(선택 안 함)"] + [j.get("job_id", "(no id)") for j in recent]
    pick = st.selectbox("최근 잡 선택", opts, index=0)
    if pick != "(선택 안 함)":
        selected_job_id = pick

# Show selected job
if selected_job_id:
    try:
        job = get_job(selected_job_id)
    except Exception as e:
        job = None
        st.error(f"잡 조회 실패: {e}")

    if not job:
        st.info("해당 job_id가 RTDB에 없습니다.")
    else:
        st.write("**Job**")
        st.json(job)

        status = (job.get("status") or "").lower().strip()
        progress = int(job.get("progress") or 0)
        st.progress(min(max(progress, 0), 100))

        if status == "done":
            out_id = job.get("outbox_file_id")
            out_name = job.get("result_filename") or f"{selected_job_id}_translated.dxf"
            if out_id:
                if st.button("Download Result DXF", use_container_width=True):
                    try:
                        data = drive_download_bytes(drive, out_id)
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
                st.warning("status=done 이지만 outbox_file_id가 없습니다. 워커의 RTDB 업데이트를 확인해 주세요.")

        elif status == "error":
            st.error(job.get("message") or "워커 처리 중 에러가 발생했습니다.")

else:
    st.info("job_id를 입력하거나 목록에서 선택하세요.")

if auto_refresh:
    time.sleep(5)
    st.rerun()
