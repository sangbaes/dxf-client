#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DXF Translation Client - Firebase RTDB Version
===============================================
Streamlit app for uploading DXF files and monitoring translation jobs via Firebase RTDB
"""

import os
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import BytesIO

import streamlit as st

# Firebase
import firebase_admin
from firebase_admin import credentials, db

# Google Drive
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# =========================
# Config
# =========================
INBOX_FOLDER_ID = os.getenv("INBOX_FOLDER_ID", "1QFhwS0aMPbwjtpC0k83ZJr8-abHksNhJ")
DONE_FOLDER_ID = os.getenv("DONE_FOLDER_ID", "1rC_1x1HAoJZ65YuGLDw8GikyBbqXWIJa")

SEOUL_TZ = timezone(timedelta(hours=9))
MAX_FILE_MB = 200
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

SCOPES = ["https://www.googleapis.com/auth/drive"]

# =========================
# Firebase Initialization
# =========================
@st.cache_resource
def init_firebase():
    """Initialize Firebase (once per app lifetime)"""
    if not firebase_admin._apps:
        # Streamlit Secrets에서 Firebase 설정 읽기
        firebase_config = dict(st.secrets["firebase"])
        
        cred = credentials.Certificate(firebase_config)
        firebase_admin.initialize_app(cred, {
            'databaseURL': st.secrets["firebase"]["databaseURL"]
        })
    
    return db.reference()

# =========================
# Google Drive Helpers
# =========================
@st.cache_resource(show_spinner=False)
def get_drive_service():
    """Get Google Drive service (cached)"""
    oauth = st.secrets["drive_oauth"]
    
    creds = Credentials(
        token=None,
        refresh_token=oauth["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=oauth["client_id"],
        client_secret=oauth["client_secret"],
        scopes=SCOPES,
    )
    creds.refresh(Request())
    
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_file_to_drive(drive, folder_id: str, filename: str, file_obj, mime: str = "application/dxf"):
    """Upload file to Google Drive"""
    try:
        file_obj.seek(0)
    except Exception:
        pass
    
    media = MediaIoBaseUpload(file_obj, mimetype=mime, resumable=False)
    metadata = {"name": filename, "parents": [folder_id]}
    
    req = drive.files().create(body=metadata, media_body=media, fields="id,name,size")
    result = req.execute()
    
    return result


def download_file_from_drive(drive, file_id: str) -> bytes:
    """Download file from Google Drive"""
    request = drive.files().get_media(fileId=file_id)
    
    file_buffer = BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    file_buffer.seek(0)
    return file_buffer.read()


def find_file_in_done_folder(drive, filename: str):
    """Find file in DONE folder by name"""
    query = f"'{DONE_FOLDER_ID}' in parents and name='{filename}' and trashed=false"
    
    try:
        results = drive.files().list(q=query, fields="files(id,name,size)").execute()
        files = results.get("files", [])
        return files[0] if files else None
    except Exception:
        return None


# =========================
# RTDB Helpers
# =========================
def create_job(ref, file_id: str, filename: str, priority: int = 50):
    """Create a new job in Firebase RTDB"""
    timestamp = datetime.now(SEOUL_TZ).strftime("%Y%m%d_%H%M%S")
    short_id = uuid.uuid4().hex[:8]
    safe_name = filename.replace(" ", "_").replace(".", "_")[:40]
    
    job_id = f"job_{timestamp}_{short_id}_{safe_name}"
    
    priority_pad = str(priority).zfill(4)
    now_ms = int(datetime.now().timestamp() * 1000)
    
    job_data = {
        "status": "queued",
        "statusKey": f"queued|{priority_pad}",
        "priority": priority,
        "createdAt": now_ms,
        "updatedAt": now_ms,
        
        "ownerUid": "streamlit_client",
        "workerUid": None,
        "claimedAt": None,
        "leaseUntil": None,
        
        "source": {
            "type": "gdrive",
            "fileId": file_id,
            "fileName": filename,
            "langFrom": "ru",
            "langTo": "en"
        },
        
        "result": {
            "outputFileId": None,
            "outputFileName": None
        },
        
        "error": {
            "code": None,
            "message": None
        }
    }
    
    jobs_ref = ref.child(f"jobs/{job_id}")
    jobs_ref.set(job_data)
    
    return job_id


def get_job_status(ref, job_id: str):
    """Get job status from RTDB"""
    job_ref = ref.child(f"jobs/{job_id}")
    return job_ref.get()


def get_all_jobs(ref, limit: int = 50):
    """Get all jobs ordered by creation time"""
    jobs_ref = ref.child("jobs")
    jobs = jobs_ref.order_by_child("createdAt").limit_to_last(limit).get()
    
    if not jobs:
        return []
    
    # Convert to list and sort by createdAt (newest first)
    job_list = [{"id": k, **v} for k, v in jobs.items()]
    job_list.sort(key=lambda x: x.get("createdAt", 0), reverse=True)
    
    return job_list


def get_worker_heartbeats(ref):
    """Get all worker heartbeats"""
    heartbeat_ref = ref.child("workerHeartbeat")
    return heartbeat_ref.get() or {}


# =========================
# Streamlit UI
# =========================
st.set_page_config(
    page_title="DXF Translation",
    page_icon="🔧",
    layout="wide"
)

st.title("🔧 DXF Translation System")
st.caption("Russian → English DXF File Translator (Firebase RTDB)")

# Initialize services
try:
    rtdb_ref = init_firebase()
    drive = get_drive_service()
    st.success("✅ Connected to Firebase RTDB and Google Drive")
except Exception as e:
    st.error(f"❌ Failed to initialize services: {e}")
    st.stop()

# =========================
# Sidebar: Worker Status
# =========================
st.sidebar.header("🤖 Worker Status")

workers = get_worker_heartbeats(rtdb_ref)
now_ms = int(datetime.now().timestamp() * 1000)

if workers:
    active_workers = []
    idle_workers = []
    
    for worker_id, data in workers.items():
        last_seen = data.get("lastSeen", 0)
        diff_sec = (now_ms - last_seen) / 1000
        
        if diff_sec < 10:
            active_workers.append((worker_id, data, diff_sec))
        elif diff_sec < 30:
            idle_workers.append((worker_id, data, diff_sec))
    
    # Active workers (green)
    if active_workers:
        st.sidebar.success(f"🟢 Active Workers: {len(active_workers)}")
        for worker_id, data, diff_sec in active_workers:
            host = data.get("host", "Unknown")
            st.sidebar.caption(f"• {host} ({int(diff_sec)}s ago)")
    
    # Idle workers (yellow)
    if idle_workers:
        st.sidebar.warning(f"🟡 Idle Workers: {len(idle_workers)}")
        for worker_id, data, diff_sec in idle_workers:
            host = data.get("host", "Unknown")
            st.sidebar.caption(f"• {host} ({int(diff_sec)}s ago)")
    
    # No active workers
    if not active_workers and not idle_workers:
        st.sidebar.error("⚠️ No active workers")
else:
    st.sidebar.error("⚠️ No workers found")

st.sidebar.markdown("---")

# Auto-refresh settings
st.sidebar.subheader("⚙️ Settings")
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
refresh_interval = st.sidebar.slider("Refresh interval (sec)", 2, 10, 3)

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()

# =========================
# Main Content: 3 Tabs
# =========================
tab_upload, tab_monitor, tab_stats = st.tabs(["📤 Upload", "📊 Monitor Jobs", "📈 Statistics"])

# =========================
# Tab 1: Upload
# =========================
with tab_upload:
    st.header("Upload DXF Files")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        uploaded_files = st.file_uploader(
            "Choose DXF file(s)",
            type=["dxf"],
            accept_multiple_files=True,
            help=f"Max {MAX_FILE_MB}MB per file"
        )
    
    with col2:
        priority = st.selectbox(
            "Priority",
            options=[25, 50, 100, 200],
            index=1,
            format_func=lambda x: {
                200: "🔴 Urgent (200)",
                100: "🟡 High (100)",
                50: "🟢 Normal (50)",
                25: "⚪ Low (25)"
            }[x]
        )
    
    if uploaded_files:
        st.info(f"📋 {len(uploaded_files)} file(s) selected")
        
        # Preview
        with st.expander("Preview selected files"):
            for f in uploaded_files:
                size_mb = len(f.getvalue()) / 1024 / 1024
                st.write(f"• {f.name} ({size_mb:.2f} MB)")
        
        if st.button("🚀 Upload & Start Translation", type="primary", use_container_width=True):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            success_count = 0
            failed_files = []
            created_jobs = []
            
            for idx, uploaded_file in enumerate(uploaded_files):
                try:
                    # Validate file size
                    file_size = len(uploaded_file.getvalue())
                    if file_size > MAX_FILE_BYTES:
                        raise ValueError(f"File too large: {file_size / 1024 / 1024:.1f}MB > {MAX_FILE_MB}MB")
                    
                    status_text.text(f"Uploading {uploaded_file.name}... ({idx + 1}/{len(uploaded_files)})")
                    
                    # Upload to Drive
                    result = upload_file_to_drive(
                        drive,
                        INBOX_FOLDER_ID,
                        uploaded_file.name,
                        uploaded_file,
                        mime=uploaded_file.type or "application/dxf"
                    )
                    
                    file_id = result["id"]
                    
                    # Create job in RTDB
                    job_id = create_job(rtdb_ref, file_id, uploaded_file.name, priority)
                    created_jobs.append((job_id, uploaded_file.name))
                    
                    success_count += 1
                    
                except Exception as e:
                    failed_files.append((uploaded_file.name, str(e)))
                
                # Update progress
                progress_bar.progress((idx + 1) / len(uploaded_files))
            
            progress_bar.empty()
            status_text.empty()
            
            # Summary
            st.success(f"✅ Successfully uploaded: {success_count}/{len(uploaded_files)}")
            
            if created_jobs:
                with st.expander("Created jobs"):
                    for job_id, filename in created_jobs:
                        st.code(f"{filename} → {job_id}")
                
                # Store first job for monitoring
                if created_jobs:
                    st.session_state["last_job_id"] = created_jobs[0][0]
            
            if failed_files:
                st.error(f"❌ Failed: {len(failed_files)}")
                with st.expander("Failed files"):
                    for filename, error in failed_files:
                        st.write(f"• {filename}: {error}")

# =========================
# Tab 2: Monitor Jobs
# =========================
with tab_monitor:
    st.header("Job Monitoring")
    
    # Get all jobs
    all_jobs = get_all_jobs(rtdb_ref, limit=50)
    
    if not all_jobs:
        st.info("No jobs yet. Upload files to get started.")
    else:
        # Filter options
        col1, col2 = st.columns([3, 1])
        
        with col1:
            filter_status = st.multiselect(
                "Filter by status",
                options=["queued", "working", "done", "error"],
                default=["queued", "working"]
            )
        
        with col2:
            if st.button("🔄 Refresh"):
                st.rerun()
        
        # Filter jobs
        filtered_jobs = [j for j in all_jobs if j.get("status") in filter_status] if filter_status else all_jobs
        
        st.caption(f"Showing {len(filtered_jobs)} of {len(all_jobs)} jobs")
        
        # Display jobs
        for job in filtered_jobs:
            job_id = job["id"]
            status = job.get("status", "unknown")
            filename = job.get("source", {}).get("fileName", "Unknown")
            priority = job.get("priority", 0)
            progress = job.get("progress", 0)
            created_at = datetime.fromtimestamp(job.get("createdAt", 0) / 1000, SEOUL_TZ)
            
            # Status badge
            status_emoji = {
                "queued": "⏳",
                "working": "🔄",
                "done": "✅",
                "error": "❌"
            }.get(status, "❓")
            
            status_color = {
                "queued": "🟡",
                "working": "🔵",
                "done": "🟢",
                "error": "🔴"
            }.get(status, "⚪")
            
            with st.expander(f"{status_emoji} {filename} - {status.upper()} (Priority: {priority})"):
                col_info, col_action = st.columns([3, 1])
                
                with col_info:
                    st.write(f"**Job ID:** `{job_id}`")
                    st.write(f"**Created:** {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    st.write(f"**Status:** {status_color} {status.upper()}")
                    
                    if status == "working":
                        worker = job.get("workerUid", "Unknown")
                        message = job.get("progressMessage", "Processing...")
                        st.write(f"**Worker:** {worker}")
                        st.write(f"**Progress:** {progress}%")
                        st.progress(progress / 100.0)
                        st.caption(message)
                    
                    if status == "error":
                        error = job.get("error", {})
                        st.error(f"Error: {error.get('message', 'Unknown error')}")
                    
                    if status == "done":
                        result = job.get("result", {})
                        output_filename = result.get("outputFileName")
                        output_file_id = result.get("outputFileId")
                        
                        if output_filename and output_file_id:
                            st.success(f"✅ Translation completed")
                            st.write(f"**Output:** {output_filename}")
                
                with col_action:
                    if status == "done":
                        result = job.get("result", {})
                        output_file_id = result.get("outputFileId")
                        output_filename = result.get("outputFileName")
                        
                        if output_file_id and output_filename:
                            if st.button("📥 Download", key=f"download_{job_id}"):
                                with st.spinner("Downloading..."):
                                    try:
                                        file_data = download_file_from_drive(drive, output_file_id)
                                        
                                        st.download_button(
                                            label="💾 Save File",
                                            data=file_data,
                                            file_name=output_filename,
                                            mime="application/dxf",
                                            key=f"save_{job_id}"
                                        )
                                    except Exception as e:
                                        st.error(f"Download failed: {e}")

# =========================
# Tab 3: Statistics
# =========================
with tab_stats:
    st.header("System Statistics")
    
    all_jobs = get_all_jobs(rtdb_ref, limit=100)
    
    # Count by status
    status_counts = {"queued": 0, "working": 0, "done": 0, "error": 0}
    for job in all_jobs:
        status = job.get("status", "unknown")
        if status in status_counts:
            status_counts[status] += 1
    
    # Display stats
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("⏳ Queued", status_counts["queued"])
    
    with col2:
        st.metric("🔄 Working", status_counts["working"])
    
    with col3:
        st.metric("✅ Done", status_counts["done"])
    
    with col4:
        st.metric("❌ Error", status_counts["error"])
    
    st.markdown("---")
    
    # Recent activity
    st.subheader("Recent Activity")
    
    if all_jobs:
        recent_5 = all_jobs[:5]
        
        for job in recent_5:
            filename = job.get("source", {}).get("fileName", "Unknown")
            status = job.get("status", "unknown")
            created_at = datetime.fromtimestamp(job.get("createdAt", 0) / 1000, SEOUL_TZ)
            
            status_emoji = {
                "queued": "⏳",
                "working": "🔄",
                "done": "✅",
                "error": "❌"
            }.get(status, "❓")
            
            st.write(f"{status_emoji} **{filename}** - {status} ({created_at.strftime('%H:%M:%S')})")
    else:
        st.info("No jobs yet")

# =========================
# Footer
# =========================
st.markdown("---")
st.caption("DXF Translation System v2.0 (Firebase RTDB) | Made with Streamlit")
