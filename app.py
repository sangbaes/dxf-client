import base64
import json
import time
import uuid
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from io import BytesIO
from datetime import datetime, timezone, timedelta

import streamlit as st

def reset_for_new_job():
    """Reset session state so the client app is ready for a new batch/job."""
    for k in [
        "active_job_id",
        "active_job_ids",
        "active_batch_id",
        "upload_progress",
        "selected_manifest",
        "zip_bytes",
        "zip_name",
    ]:
        st.session_state.pop(k, None)

    # Reset file uploader by bumping its key
    st.session_state["uploader_key"] = st.session_state.get("uploader_key", 0) + 1

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
    safe = "".join(c for c in original_name if c.isalnum() or c in ("-", "_", "."))
    safe = safe[:40] if safe else "file"
    return f"{ts}_{short}_{safe}"



def drive_execute(req, retries: int = 5, base_sleep: float = 0.6):
    """Drive API 요청을 네트워크 흔들림에도 최대한 견디도록 재시도 실행."""
    last_err = None
    for i in range(retries + 1):
        try:
            # googleapiclient 자체 재시도도 있지만, SSL read/connection reset은 직접 감싸주는 게 더 안정적임
            return req.execute(num_retries=1)
        except (HttpError, OSError, ssl.SSLError, socket.timeout) as e:
            last_err = e
            if i >= retries:
                raise
            time.sleep(base_sleep * (2 ** i))
    raise last_err


def load_service_account_info():
    # ✅ Base64 방식 (Streamlit Secrets: SERVICE_ACCOUNT_B64)
    if "SERVICE_ACCOUNT_B64" not in st.secrets:
        raise RuntimeError("Streamlit Secrets에 SERVICE_ACCOUNT_B64가 없습니다.")

    raw = base64.b64decode(st.secrets["SERVICE_ACCOUNT_B64"].encode("ascii"))
    info = json.loads(raw.decode("utf-8"))

    # 방어: 혹시 \\n로 저장된 경우 실제 줄바꿈으로 복구
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n").strip()

    return info

    if "SERVICE_ACCOUNT_JSON" in st.secrets:
        info = json.loads(st.secrets["SERVICE_ACCOUNT_JSON"])
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n").strip()
        return info

    raise RuntimeError(
        "Streamlit Secrets에 gcp_service_account 또는 SERVICE_ACCOUNT_JSON이 없습니다."
    )

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
    # access_token이 필요할 때 자동 갱신
    creds.refresh(Request())

    # Streamlit Cloud에서 간헐적으로 발생하는 SSL/네트워크 read 문제를 완화하기 위해
    # - httplib2 timeout 지정
    # - AuthorizedHttp 사용
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
    # cache in session to avoid repeated API calls
    if "subfolder_ids" in st.session_state:
        return st.session_state["subfolder_ids"]

    ids = {}
    for name in SUBFOLDERS:
        ids[name] = find_or_create_folder(drive, DXF_SHARED_FOLDER_ID, name)

    st.session_state["subfolder_ids"] = ids
    return ids


def upload_file_to_folder(drive, folder_id: str, filename: str, file_bytes: bytes, mime: str):
    media = MediaIoBaseUpload(BytesIO(file_bytes), mimetype=mime, resumable=True)
    metadata = {"name": filename, "parents": [folder_id]}
    req = drive.files().create(body=metadata, media_body=media, fields="id,name,size,createdTime")
    resp = None

    # Resumable upload loop
    while resp is None:
        status, resp = req.next_chunk()
        if status:
            st.session_state["upload_progress"] = int(status.progress() * 100)

    return resp


def upsert_json_file(drive, folder_id: str, filename: str, payload: dict):
    # Find existing
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
        # Drive API가 일시적으로 흔들릴 때 앱이 죽지 않게 방어
        st.warning(
            "Google Drive 조회가 일시적으로 실패했습니다. 잠시 후 자동으로 다시 시도합니다.\n"
            f"원인: {type(e).__name__}"
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


# =========================
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

st.title("DXF Client")


col_new1, col_new2 = st.columns([1, 3])
with col_new1:
    if st.button("🆕 새 작업 시작"):
        reset_for_new_job()
        st.rerun()
with col_new2:
    st.caption("새 작업을 시작합니다. (이전 작업 재다운로드는 모니터링 앱에서 제공 예정)")

with st.expander("설명", expanded=False):
    st.markdown(
        """
- 이 앱은 **Google Drive 공유폴더에 DXF를 업로드**하고,
- MacBook Pro 로컬 워커가 번역 후 결과를 `DONE/`에 올리면,
- 앱이 완료를 감지해 **다운로드 버튼**을 제공합니다.
        """
    )

# Drive connection
try:
    drive = get_drive_service()
    folders = get_subfolder_ids(drive)
except Exception as e:
    st.error("Google Drive 연결/폴더 초기화에 실패했습니다. Secrets 또는 폴더 공유 권한을 확인하세요.")
    st.exception(e)
    st.stop()

st.success("✅ Google Drive 연결됨")
st.caption(f"DXF_SHARED_FOLDER_ID = {DXF_SHARED_FOLDER_ID}")

# Sidebar controls
st.sidebar.header("옵션")
auto_refresh = st.sidebar.checkbox("상태 자동 새로고침", value=True)
refresh_sec = st.sidebar.slider("새로고침 주기(초)", 3, 30, 5)

st.sidebar.divider()
st.sidebar.caption("폴더")
for k in SUBFOLDERS:
    st.sidebar.write(f"- {k}: `{folders[k]}`")

# Upload section
st.subheader("1) DXF 업로드")
uploaded_list = uploaded_list = st.file_uploader(
    "DXF 파일 선택 (여러 개 가능)",
    type=["dxf"],
    accept_multiple_files=True,
    key=f"uploader_{st.session_state.get('uploader_key', 0)}",
)
", type=["dxf"], accept_multiple_files=True, key=f"uploader_{st.session_state.get('uploader_key', 0)}")

if uploaded_list is not None:
    size = uploaded_list.size  # bytes
    st.write(f"파일명: `{uploaded_list.name}` / 크기: {size/1024/1024:.1f} MB")

    if size > MAX_FILE_BYTES:
        st.error(f"파일이 너무 큽니다. {MAX_FILE_MB}MB 이하만 업로드할 수 있습니다.")
    else:
        if st.button("INBOX로 업로드", type="primary"):
            st.session_state.pop("upload_progress", None)
            file_bytes = uploaded_list.getvalue()

            job_id = make_job_id(uploaded_list.name)
            inbox_name = f"{job_id}__{uploaded_list.name}"

            meta_filename = f"{job_id}.json"
            meta_payload = {
                "job_id": job_id,
                "original_name": uploaded_list.name,
                "inbox_name": inbox_name,
                "status": "queued",  # queued | working | done | error
                "created_at": now_seoul_iso(),
                "updated_at": now_seoul_iso(),
                "progress": 0,
                "message": "Uploaded to INBOX. Waiting for local worker.",
                "done_file": None,
                "error": None,
            }

            with st.spinner("업로드 중..."):
                try:
                    resp = upload_file_to_folder(
                        drive,
                        folders["INBOX"],
                        inbox_name,
                        file_bytes,
                        mime="application/dxf"
                    )
                    meta_payload["inbox_file_id"] = resp.get("id")
                    meta_payload["progress"] = 5
                    meta_payload["updated_at"] = now_seoul_iso()

                    upsert_json_file(drive, folders["META"], meta_filename, meta_payload)

                    st.success("✅ 업로드 완료")
                    st.code(f"job_id: {job_id}")
                    st.session_state["active_job_id"] = job_id

                except Exception as e:
                    st.error("❌ 업로드 실패")
                    st.exception(e)

# Progress indicator (upload)
if "upload_progress" in st.session_state:
    st.progress(st.session_state["upload_progress"] / 100.0)

st.divider()

# Job monitor
st.subheader("2) 작업 상태 / 다운로드")

# Load recent jobs for selection
recent = list_recent_jobs(drive, folders["META"], limit=30)
recent_ids = [f["name"].replace(".json", "") for f in recent]

default_job = st.session_state.get("active_job_id")
if default_job and default_job in recent_ids:
    default_index = recent_ids.index(default_job)
else:
    default_index = 0 if recent_ids else None

job_id = None
if recent_ids:
    job_id = st.selectbox("최근 작업 선택", recent_ids, index=default_index)
else:
    st.info("META 폴더에 작업이 아직 없습니다. 먼저 업로드하세요.")

# Auto refresh
def do_autorefresh():
    # streamlit has st_autorefresh in many versions
    try:
        from streamlit import st_autorefresh
        st_autorefresh(interval=refresh_sec * 1000, key="job_poll")
    except Exception:
        # fallback: user can press button
        pass

if auto_refresh:
    do_autorefresh()

col_a, col_b = st.columns([1, 1])
with col_a:
    manual_refresh = st.button("상태 새로고침")
with col_b:
    st.caption("자동 새로고침이 안 되면 버튼을 사용하세요.")

if job_id:
    meta_name = f"{job_id}.json"
    meta = None
    try:
        meta = read_json_file_by_name(drive, folders["META"], meta_name)
    except Exception as e:
        st.error("META 읽기 실패")
        st.exception(e)

    if meta:
        st.write(f"**status:** `{meta.get('status')}`")
        st.write(f"**updated_at:** `{meta.get('updated_at')}`")
        st.write(f"**message:** {meta.get('message')}")
        prog = int(meta.get("progress", 0) or 0)
        st.progress(min(max(prog, 0), 100) / 100.0)

        if meta.get("status") == "error":
            st.error("작업 실패")
            if meta.get("error"):
                st.code(meta.get("error"))

        if meta.get("status") == "done":
            done_file = meta.get("done_file")
            if not done_file:
                st.warning("done 상태지만 done_file 정보가 META에 없습니다.")
            else:
                st.success("✅ 번역 완료")
                st.write(f"결과 파일: `{done_file}`")

                done_obj = find_file_in_folder_by_name(drive, folders["DONE"], done_file)
                if not done_obj:
                    st.warning("DONE 폴더에서 결과 파일을 아직 찾지 못했습니다. 잠시 후 다시 시도하세요.")
                else:
                    # Download through API and offer download button
                    try:
                        with st.spinner("결과 파일 다운로드 준비 중... (파일이 크면 시간이 걸릴 수 있습니다)"):
                            data = download_file_bytes(drive, done_obj["id"])
                        st.download_button(
                            label="결과 DXF 다운로드",
                            data=data,
                            file_name=done_file,
                            mime="application/dxf",
                            type="primary",
                        )
                    except Exception as e:
                        st.error("결과 파일 다운로드 준비 실패")
                        st.exception(e)

    else:
        st.info("해당 job의 META 파일을 아직 찾지 못했습니다.")