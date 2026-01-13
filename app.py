#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DXF Translation Client (Simplified)
====================================
멀티 파일 업로드 시 각 파일을 독립적인 job으로 처리
배치 개념 제거, 단순하고 안정적인 구조
"""

import json
import time
import uuid
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List

import streamlit as st
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
import httplib2
from google_auth_httplib2 import AuthorizedHttp


# =========================
# Config
# =========================
DXF_SHARED_FOLDER_ID = "1qhx_xTGdOusxhV0xN2df4Kc8JTfh3zTd"
SUBFOLDERS = ["INBOX", "DONE", "META"]
MAX_FILE_MB = 200
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

SEOUL_TZ = timezone(timedelta(hours=9))
SCOPES = ["https://www.googleapis.com/auth/drive"]


# =========================
# Helper Functions
# =========================
def now_seoul_iso() -> str:
    """현재 시간 ISO 포맷 (서울 시간대)"""
    return datetime.now(SEOUL_TZ).isoformat(timespec="seconds")


def make_job_id(original_name: str) -> str:
    """고유 job_id 생성: YYYYMMDD_HHMMSS_uuid8_filename"""
    ts = datetime.now(SEOUL_TZ).strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    safe_name = "".join(c for c in original_name if c.isalnum() or c in ("-", "_", "."))
    safe_name = safe_name[:40] if safe_name else "file"
    return f"{ts}_{short_uuid}_{safe_name}"


def sanitize_filename(filename: str) -> str:
    """파일명 정리 (경로 공격 방지)"""
    return Path(filename).name


def format_bytes(bytes_size: int) -> str:
    """바이트를 읽기 쉬운 포맷으로 변환"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f}{unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f}TB"


# =========================
# Drive API (안정화된 버전)
# =========================
@st.cache_resource(show_spinner=False)
def get_drive_service():
    """Drive API 서비스 생성 (OAuth, timeout 설정)"""
    try:
        cfg = st.secrets["drive_oauth"]
        creds = Credentials(
            token=None,
            refresh_token=cfg["refresh_token"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=cfg["client_id"],
            client_secret=cfg["client_secret"],
            scopes=SCOPES,
        )
        creds.refresh(Request())
        
        # httplib2 timeout 설정 (네트워크 안정성)
        authed_http = AuthorizedHttp(creds, http=httplib2.Http(timeout=30))
        return build("drive", "v3", http=authed_http, cache_discovery=False)
    except Exception as e:
        st.error(f"❌ Drive API 초기화 실패: {e}")
        st.stop()


def drive_api_call(func, retries=3, base_delay=1.0):
    """
    Drive API 호출을 안정적으로 실행 (재시도 로직)
    
    Args:
        func: 실행할 함수 (lambda 등)
        retries: 최대 재시도 횟수
        base_delay: 기본 대기 시간(초)
    
    Returns:
        API 호출 결과
    """
    last_error = None
    for attempt in range(retries + 1):
        try:
            return func()
        except (HttpError, OSError, Exception) as e:
            last_error = e
            if attempt >= retries:
                raise
            # Exponential backoff
            delay = base_delay * (2 ** attempt)
            time.sleep(delay)
    raise last_error


def find_or_create_folder(drive, parent_id: str, name: str) -> str:
    """폴더 찾기 또는 생성"""
    def _find():
        q = (
            f"'{parent_id}' in parents and "
            f"name = '{name}' and "
            "mimeType = 'application/vnd.google-apps.folder' and "
            "trashed = false"
        )
        res = drive.files().list(q=q, fields="files(id,name)").execute()
        return res.get("files", [])
    
    files = drive_api_call(_find)
    if files:
        return files[0]["id"]
    
    # 폴더 생성
    def _create():
        metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = drive.files().create(body=metadata, fields="id").execute()
        return folder["id"]
    
    return drive_api_call(_create)


def get_subfolder_ids(drive):
    """서브폴더 ID 가져오기 (캐시 활용)"""
    if "subfolder_ids" in st.session_state:
        return st.session_state["subfolder_ids"]
    
    ids = {}
    for name in SUBFOLDERS:
        ids[name] = find_or_create_folder(drive, DXF_SHARED_FOLDER_ID, name)
    
    st.session_state["subfolder_ids"] = ids
    return ids


def upload_file_to_inbox(drive, inbox_folder_id: str, filename: str, file_bytes: bytes) -> Dict:
    """
    INBOX 폴더에 파일 업로드 (Resumable)
    
    Returns:
        {"id": file_id, "name": filename, "size": bytes}
    """
    media = MediaIoBaseUpload(BytesIO(file_bytes), mimetype="application/dxf", resumable=True)
    metadata = {"name": filename, "parents": [inbox_folder_id]}
    
    def _upload():
        req = drive.files().create(body=metadata, media_body=media, fields="id,name,size")
        resp = None
        while resp is None:
            status, resp = req.next_chunk()
            if status:
                # 진행률 업데이트 (선택적)
                pass
        return resp
    
    return drive_api_call(_upload, retries=5)


def create_meta_json(drive, meta_folder_id: str, meta_filename: str, payload: Dict):
    """
    META 폴더에 JSON 파일 생성
    
    Note: 기존 파일이 있으면 덮어쓰기
    """
    # 기존 파일 검색
    def _find():
        q = (
            f"'{meta_folder_id}' in parents and "
            f"name = '{meta_filename}' and "
            "trashed = false"
        )
        res = drive.files().list(q=q, fields="files(id)").execute()
        return res.get("files", [])
    
    existing = drive_api_call(_find)
    
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    media = MediaIoBaseUpload(BytesIO(data), mimetype="application/json", resumable=False)
    
    if existing:
        # 업데이트
        file_id = existing[0]["id"]
        def _update():
            return drive.files().update(fileId=file_id, media_body=media).execute()
        return drive_api_call(_update)
    else:
        # 생성
        def _create():
            meta = {"name": meta_filename, "parents": [meta_folder_id]}
            return drive.files().create(body=meta, media_body=media, fields="id").execute()
        return drive_api_call(_create)


def list_recent_jobs(drive, meta_folder_id: str, limit: int = 30):
    """최근 작업 목록 가져오기"""
    def _list():
        q = f"'{meta_folder_id}' in parents and trashed=false"
        res = drive.files().list(
            q=q,
            fields="files(id,name,modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=limit,
        ).execute()
        files = res.get("files", [])
        return [f for f in files if f["name"].lower().endswith(".json")]
    
    try:
        return drive_api_call(_list)
    except Exception as e:
        st.warning(f"⚠️ 작업 목록 조회 실패 (잠시 후 재시도): {type(e).__name__}")
        return []


def read_meta_json(drive, meta_folder_id: str, meta_filename: str) -> Optional[Dict]:
    """META JSON 파일 읽기"""
    def _find():
        q = (
            f"'{meta_folder_id}' in parents and "
            f"name = '{meta_filename}' and "
            "trashed = false"
        )
        res = drive.files().list(q=q, fields="files(id)").execute()
        return res.get("files", [])
    
    files = drive_api_call(_find)
    if not files:
        return None
    
    file_id = files[0]["id"]
    
    def _download():
        req = drive.files().get_media(fileId=file_id)
        buf = BytesIO()
        downloader = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        return json.loads(buf.read().decode("utf-8"))
    
    return drive_api_call(_download)


def find_done_file(drive, done_folder_id: str, filename: str):
    """DONE 폴더에서 파일 찾기"""
    def _find():
        q = (
            f"'{done_folder_id}' in parents and "
            f"name = '{filename}' and "
            "trashed = false"
        )
        res = drive.files().list(q=q, fields="files(id,name,size,modifiedTime)").execute()
        return res.get("files", [])
    
    files = drive_api_call(_find)
    return files[0] if files else None


def download_file_bytes(drive, file_id: str) -> bytes:
    """파일 다운로드 (바이트)"""
    def _download():
        req = drive.files().get_media(fileId=file_id)
        buf = BytesIO()
        downloader = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()
    
    return drive_api_call(_download, retries=5)


# =========================
# Streamlit UI
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

st.title("🔧 DXF Translation Client")

# 설명
with st.expander("📖 사용 방법", expanded=False):
    st.markdown("""
### 작동 방식
1. **DXF 파일 업로드**: 여러 파일을 선택 가능 (각각 독립 작업으로 처리)
2. **자동 번역**: 로컬 워커가 번역 수행
3. **결과 다운로드**: 완료되면 다운로드 버튼 표시

### 특징
- ✅ 각 파일은 독립적인 작업으로 처리
- ✅ 한 파일이 실패해도 다른 파일에 영향 없음
- ✅ 자동 새로고침으로 진행 상황 확인
    """)

# Drive 연결
drive = get_drive_service()
folders = get_subfolder_ids(drive)

st.success("✅ Google Drive 연결됨")
st.caption(f"공유 폴더: `{DXF_SHARED_FOLDER_ID}`")

# =========================
# Sidebar
# =========================
st.sidebar.header("⚙️ 옵션")
auto_refresh = st.sidebar.checkbox("자동 새로고침", value=True)
refresh_sec = st.sidebar.slider("새로고침 주기(초)", 3, 30, 5)

st.sidebar.divider()
st.sidebar.caption("📁 폴더 ID")
for name in SUBFOLDERS:
    st.sidebar.write(f"- {name}: `{folders[name][:12]}...`")

# =========================
# 1) 파일 업로드
# =========================
st.subheader("1️⃣ DXF 파일 업로드")

uploaded_files = st.file_uploader(
    "DXF 파일 선택 (여러 개 가능)",
    type=["dxf"],
    accept_multiple_files=True,
    help="각 파일은 독립적인 작업으로 처리됩니다"
)

if uploaded_files:
    total_count = len(uploaded_files)
    total_size = sum(f.size for f in uploaded_files)
    
    st.write(f"**선택된 파일**: {total_count}개 | **총 크기**: {format_bytes(total_size)}")
    
    # 크기 체크
    oversized = [f for f in uploaded_files if f.size > MAX_FILE_BYTES]
    
    if oversized:
        st.error(f"❌ 다음 파일이 {MAX_FILE_MB}MB를 초과합니다:")
        for f in oversized:
            st.write(f"  - {f.name} ({format_bytes(f.size)})")
    else:
        # 업로드 버튼
        if st.button("📤 업로드 시작", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            uploaded_jobs = []
            failed_jobs = []
            created_at = now_seoul_iso()
            
            with st.spinner("업로드 중..."):
                for idx, uploaded_file in enumerate(uploaded_files, 1):
                    try:
                        # 파일명 정리
                        safe_name = sanitize_filename(uploaded_file.name)
                        file_bytes = uploaded_file.getvalue()
                        
                        # job_id 생성
                        job_id = make_job_id(safe_name)
                        
                        # INBOX 파일명: job_id__원본명.dxf
                        inbox_name = f"{job_id}__{safe_name}"
                        
                        # META 파일명: job_id.json
                        meta_filename = f"{job_id}.json"
                        
                        status_text.text(f"[{idx}/{total_count}] {safe_name} 업로드 중...")
                        
                        # 1) INBOX에 DXF 업로드
                        inbox_resp = upload_file_to_inbox(
                            drive,
                            folders["INBOX"],
                            inbox_name,
                            file_bytes
                        )
                        
                        # 2) META JSON 생성
                        meta_payload = {
                            "job_id": job_id,
                            "original_name": safe_name,
                            "inbox_name": inbox_name,
                            "inbox_file_id": inbox_resp.get("id"),
                            "status": "queued",
                            "progress": 0,
                            "message": "Uploaded to INBOX. Waiting for worker.",
                            "created_at": created_at,
                            "updated_at": now_seoul_iso(),
                            "done_file": None,
                            "error": None,
                        }
                        
                        create_meta_json(
                            drive,
                            folders["META"],
                            meta_filename,
                            meta_payload
                        )
                        
                        uploaded_jobs.append({
                            "job_id": job_id,
                            "original_name": safe_name,
                        })
                        
                    except Exception as e:
                        failed_jobs.append({
                            "file": uploaded_file.name,
                            "error": str(e)
                        })
                    
                    # 진행률 업데이트
                    progress = int((idx / total_count) * 100)
                    progress_bar.progress(progress)
            
            # 결과 표시
            st.success(f"✅ 업로드 완료: {len(uploaded_jobs)}개")
            
            if failed_jobs:
                st.error(f"❌ 업로드 실패: {len(failed_jobs)}개")
                for fail in failed_jobs:
                    st.write(f"  - {fail['file']}: {fail['error']}")
            
            # 업로드된 job_id 표시
            if uploaded_jobs:
                st.write("**생성된 작업:**")
                for job in uploaded_jobs:
                    st.code(f"{job['job_id']} ({job['original_name']})")
                
                # 세션에 마지막 업로드 job 저장 (선택 편의)
                st.session_state["last_uploaded_job"] = uploaded_jobs[-1]["job_id"]

# =========================
# 2) 작업 모니터링
# =========================
st.subheader("2️⃣ 작업 상태 확인")

# 최근 작업 목록 가져오기
recent_jobs = list_recent_jobs(drive, folders["META"], limit=30)
job_ids = [f["name"].replace(".json", "") for f in recent_jobs]

# 기본 선택: 마지막 업로드한 job
default_job = st.session_state.get("last_uploaded_job")
default_index = 0
if default_job and default_job in job_ids:
    default_index = job_ids.index(default_job)

selected_job = None
if job_ids:
    selected_job = st.selectbox(
        "작업 선택",
        job_ids,
        index=default_index,
        help="최근 30개 작업 표시"
    )
else:
    st.info("📭 아직 업로드된 작업이 없습니다.")

# 자동 새로고침
if auto_refresh:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=refresh_sec * 1000, key="auto_refresh")
    except ImportError:
        pass

# 수동 새로고침 버튼
col1, col2 = st.columns([1, 3])
with col1:
    if st.button("🔄 새로고침"):
        st.rerun()

# 선택된 작업 상세 정보
if selected_job:
    meta_filename = f"{selected_job}.json"
    meta = read_meta_json(drive, folders["META"], meta_filename)
    
    if meta:
        status = meta.get("status", "unknown")
        progress = int(meta.get("progress", 0) or 0)
        message = meta.get("message", "")
        
        # 상태 표시
        st.write(f"**상태**: `{status}`")
        st.write(f"**메시지**: {message}")
        st.write(f"**업데이트**: {meta.get('updated_at', 'N/A')}")
        
        # 진행률 바
        st.progress(min(max(progress, 0), 100) / 100.0)
        
        # 에러 표시
        if status == "error":
            st.error("❌ 작업 실패")
            if meta.get("error"):
                with st.expander("에러 상세"):
                    st.code(meta.get("error"))
        
        # 완료 시 다운로드
        if status == "done":
            done_file = meta.get("done_file")
            
            if not done_file:
                st.warning("⚠️ 완료 상태이지만 done_file 정보가 없습니다.")
            else:
                st.success("✅ 번역 완료!")
                st.write(f"**결과 파일**: `{done_file}`")
                
                # DONE 폴더에서 파일 찾기
                done_obj = find_done_file(drive, folders["DONE"], done_file)
                
                if not done_obj:
                    st.warning("⚠️ 결과 파일을 DONE 폴더에서 찾을 수 없습니다. 잠시 후 다시 시도하세요.")
                else:
                    # 다운로드 버튼
                    with st.spinner("결과 파일 다운로드 준비 중..."):
                        try:
                            file_data = download_file_bytes(drive, done_obj["id"])
                            
                            st.download_button(
                                label="📥 결과 DXF 다운로드",
                                data=file_data,
                                file_name=done_file,
                                mime="application/dxf",
                                type="primary",
                            )
                        except Exception as e:
                            st.error(f"❌ 다운로드 준비 실패: {e}")
    else:
        st.info("📄 선택한 작업의 메타 정보를 찾을 수 없습니다.")
