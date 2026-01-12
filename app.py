import base64
import json
import time
import uuid
import zipfile
from io import BytesIO
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import streamlit as st
import httplib2
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google_auth_httplib2 import AuthorizedHttp


from googleapiclient.errors import HttpError

def _http_error_details(e: HttpError) -> str:
    try:
        return (getattr(e, "content", b"") or b"").decode("utf-8", errors="ignore")
    except Exception:
        return ""

def execute_with_retries(request, retries: int = 6, base_sleep: float = 1.0):
    """Execute a googleapiclient request with exponential backoff.
    Retries transient errors (429/5xx/409), some 403 rate-limit cases, and common network errors.
    """
    last_err = None
    for i in range(retries):
        try:
            return request.execute()
        except (BrokenPipeError, ConnectionError, TimeoutError, OSError) as e:
            last_err = e
            if i == retries - 1:
                raise
            time.sleep(base_sleep * (2 ** i))
        except HttpError as e:
            last_err = e
            status = getattr(e.resp, "status", None)
            body = _http_error_details(e)
            retryable = status in (429, 500, 502, 503, 504, 409)
            if status == 403 and ("rateLimitExceeded" in body or "userRateLimitExceeded" in body):
                retryable = True
            if (not retryable) or (i == retries - 1):
                raise
            time.sleep(base_sleep * (2 ** i))
    raise last_err

# =========================================================
# Config
# =========================================================
SEOUL_TZ = datetime.now().astimezone().tzinfo  # Streamlit Cloud에서도 로컬 tz가 달라질 수 있어 간단히
SCOPES = ["https://www.googleapis.com/auth/drive"]

MAX_FILE_MB = 50
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

INBOX_FOLDER_ID = st.secrets.get("INBOX_FOLDER_ID", "1QFhwS0aMPbwjtpC0k83ZJr8-abHksNhJ")
DONE_FOLDER_ID  = st.secrets.get("DONE_FOLDER_ID",  "1rC_1x1HAoJZ65YuGLDw8GikyBbqXWIJa")
META_FOLDER_ID  = st.secrets.get("META_FOLDER_ID",  "1x2YCQTPOd5KC4tZdwfmX8zf7NZNO6Y_w")

SUBFOLDERS = {"INBOX": INBOX_FOLDER_ID, "DONE": DONE_FOLDER_ID, "META": META_FOLDER_ID}

# =========================================================
# Session reset
# =========================================================
def reset_for_new_job():
    for k in [
        "active_job_id",
        "active_job_ids",
        "active_batch_id",
        "upload_progress",
        "selected_manifest_id",
        "selected_manifest_name",
        "zip_bytes",
        "zip_name",
    ]:
        st.session_state.pop(k, None)
    st.session_state["uploader_key"] = st.session_state.get("uploader_key", 0) + 1

# =========================================================
# Helpers
# =========================================================
def now_seoul_iso() -> str:
    # ISO seconds
    return datetime.now().isoformat(timespec="seconds")

def make_job_id(original_name: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:8]
    safe = "".join(c for c in original_name if c.isalnum() or c in ("-", "_", "."))
    safe = safe[:40] if safe else "file"
    return f"{ts}_{short}_{safe}"

def make_batch_id() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{uuid.uuid4().hex[:8]}"

def get_service_account_info() -> dict:
    # 1) Streamlit 표준 방식: [gcp_service_account]
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n").strip()
        return info

    # 2) 우리가 쓰던 B64/JSON 방식
    if "SERVICE_ACCOUNT_B64" in st.secrets:
        raw = base64.b64decode(st.secrets["SERVICE_ACCOUNT_B64"].encode("ascii"))
        info = json.loads(raw.decode("utf-8"))
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n").strip()
        return info

    if "SERVICE_ACCOUNT_JSON" in st.secrets:
        info = json.loads(st.secrets["SERVICE_ACCOUNT_JSON"])
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n").strip()
        return info

    st.error("❌ Google Drive 서비스계정 Secrets가 없습니다.")
    st.info("Streamlit Cloud → Settings → Secrets에 SERVICE_ACCOUNT_JSON 또는 [gcp_service_account]를 추가하세요.")
    st.stop()



@st.cache_resource(show_spinner=False)
def get_drive():
    info = get_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    # httplib2 timeout 확장 (SSL read 끊김 완화)
    authed_http = AuthorizedHttp(creds, http=httplib2.Http(timeout=90))
    return build("drive", "v3", http=authed_http, cache_discovery=False)

def upload_file_to_folder(drive, folder_id: str, filename: str, file_bytes: bytes, mime: str):
    media = MediaIoBaseUpload(BytesIO(file_bytes), mimetype=mime, resumable=False)
    body = {"name": filename, "parents": [folder_id]}
    return execute_with_retries(drive.files().create(body=body, media_body=media, fields=\"id,name\", supportsAllDrives=True))

def download_file_bytes(drive, file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()

def upsert_json_file(drive, folder_id: str, filename: str, payload: dict):
    q = (
        f"'{folder_id}' in parents and "
        f"name = '{filename}' and "
        "mimeType != 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    res = execute_with_retries(drive.files().list(q=q, fields=\"files(id,name)\", supportsAllDrives=True, includeItemsFromAllDrives=True))
    files = res.get("files", [])
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    media = MediaIoBaseUpload(BytesIO(data), mimetype="application/json", resumable=False)

    if files:
        file_id = files[0]["id"]
        return execute_with_retries(drive.files().update(fileId=file_id, media_body=media, fields=\"id,name\", supportsAllDrives=True))
    else:
        body = {"name": filename, "parents": [folder_id]}
        return execute_with_retries(drive.files().create(body=body, media_body=media, fields=\"id,name\", supportsAllDrives=True))

def find_file_by_name(drive, folder_id: str, filename: str) -> Optional[dict]:
    q = (
        f"'{folder_id}' in parents and "
        f"name = '{filename}' and "
        "mimeType != 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    res = execute_with_retries(drive.files().list(q=q, fields=\"files(id,name,modifiedTime,size)\", supportsAllDrives=True, includeItemsFromAllDrives=True))
    files = res.get("files", [])
    return files[0] if files else None

def list_manifest_files(drive, folder_id: str, limit: int = 50) -> List[dict]:
    # name contains '__manifest.json'
    q = (
        f"'{folder_id}' in parents and "
        "name contains '__manifest.json' and "
        "trashed = false"
    )
    res = execute_with_retries(drive.files().list(q=q, pageSize=min(limit, 100), fields=\"files(id,name,modifiedTime),nextPageToken\", orderBy=\"modifiedTime desc\", supportsAllDrives=True, includeItemsFromAllDrives=True))
    return res.get("files", [])

def read_json_file_by_id(drive, file_id: str) -> dict:
    raw = download_file_bytes(drive, file_id)
    return json.loads(raw.decode("utf-8"))

def safe_basename(name: str) -> str:
    # 파일명에서 경로문자 제거
    return name.split("/")[-1].split("\\")[-1]

# =========================================================
# UI
# =========================================================
st.set_page_config(page_title="DXF Client", layout="wide")
st.title("DXF Client (Batch Upload + ZIP Download)")

col_new1, col_new2 = st.columns([1, 3])
with col_new1:
    if st.button("🆕 새 작업 시작"):
        reset_for_new_job()
        st.rerun()
with col_new2:
    st.caption("새 작업을 시작합니다. (지난 작업 재다운로드/히스토리는 모니터링 앱에서 제공)")

drive = get_drive()
folders = SUBFOLDERS

# Sidebar controls
st.sidebar.header("옵션")
auto_refresh = st.sidebar.checkbox("상태 자동 새로고침", value=True)
refresh_sec = st.sidebar.slider("새로고침 주기(초)", 3, 30, 5)

st.sidebar.divider()
st.sidebar.caption("폴더")
for k, v in folders.items():
    st.sidebar.write(f"- {k}: `{v}`")

# =========================================================
# 1) Upload (Batch)
# =========================================================
st.subheader("1) DXF 업로드 (여러 개)")
uploaded_list = st.file_uploader(
    "DXF 파일 선택 (여러 개 가능)",
    type=["dxf"],
    accept_multiple_files=True,
    key=f"uploader_{st.session_state.get('uploader_key', 0)}",
)

if uploaded_list:
    total_files = len(uploaded_list)
    total_size = sum(u.size for u in uploaded_list)
    st.write(f"선택된 파일: **{total_files}개** / 총 크기: **{total_size/1024/1024:.1f} MB**")

    too_big = [u for u in uploaded_list if u.size > MAX_FILE_BYTES]
    if too_big:
        st.error(
            "아래 파일이 너무 큽니다. "
            f"{MAX_FILE_MB}MB 이하만 업로드할 수 있습니다:\n- "
            + "\n- ".join([f"{u.name} ({u.size/1024/1024:.1f}MB)" for u in too_big])
        )
    else:
        if st.button("INBOX로 일괄 업로드", type="primary"):
            batch_id = make_batch_id()
            created_at = now_seoul_iso()

            manifest_name = f"{batch_id}__manifest.json"
            manifest_payload = {
                "batch_id": batch_id,
                "status": "uploading",  # uploading | queued | done | error
                "created_at": created_at,
                "updated_at": created_at,
                "total": total_files,
                "items": [],
                "message": "Uploading files to INBOX and writing META items."
            }

            prog = st.progress(0)
            status_box = st.empty()

            ok_count = 0
            errors = []

            with st.spinner("업로드 중..."):
                for idx, up in enumerate(uploaded_list, 1):
                    try:
                        orig_name = safe_basename(up.name)
                        file_bytes = up.getvalue()

                        job_id = make_job_id(orig_name)
                        inbox_name = f"{job_id}__{orig_name}"
                        meta_filename = f"{job_id}.json"

                        meta_payload = {
                            "batch_id": batch_id,
                            "job_id": job_id,
                            "original_name": orig_name,
                            "inbox_name": inbox_name,
                            "status": "queued",
                            "created_at": created_at,
                            "updated_at": now_seoul_iso(),
                            "progress": 0,
                            "message": "Uploaded to INBOX. Waiting for local worker.",
                            "done_file": None,
                            "error": None,
                        }

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

                        manifest_payload["items"].append({
                            "job_id": job_id,
                            "meta_filename": meta_filename,
                            "original_name": orig_name,
                            "inbox_name": inbox_name,
                            "inbox_file_id": meta_payload.get("inbox_file_id"),
                            "status": "queued",
                        })
                        ok_count += 1

                    except Exception as e:
                        errors.append({"file": up.name, "error": str(e)})

                    pct = int((idx / total_files) * 100)
                    prog.progress(pct)
                    status_box.write(f"업로드 진행: {idx}/{total_files} (성공 {ok_count} / 실패 {len(errors)})")

            manifest_payload["updated_at"] = now_seoul_iso()
            if errors:
                manifest_payload["status"] = "error"
                manifest_payload["message"] = f"Uploaded with errors: {len(errors)} failed."
                manifest_payload["errors"] = errors
            else:
                manifest_payload["status"] = "queued"
                manifest_payload["message"] = "All files uploaded. Waiting for local worker."

            try:
                upsert_json_file(drive, folders["META"], manifest_name, manifest_payload)
            except HttpError as e:
                st.error("❌ META에 manifest 저장 실패 (Drive API)")
                st.write("HTTP status:", getattr(e.resp, "status", None))
                st.code(_http_error_details(e) or "(no error body)")
                raise

            st.session_state["active_batch_id"] = batch_id
            st.session_state["active_job_ids"] = [it["job_id"] for it in manifest_payload["items"]]

            if errors:
                st.warning(f"⚠️ 일부 업로드 실패: {len(errors)}개")
                st.json(errors)
            st.success("✅ 배치 업로드 완료")
            st.code(f"batch_id: {batch_id}")

st.divider()

# =========================================================
# 2) Status / Download (Batch)
# =========================================================
st.subheader("2) 상태 확인 & ZIP 다운로드")

# choose manifest
manifests = list_manifest_files(drive, folders["META"], limit=50)
default_idx = 0
selected_manifest = None

# if active_batch_id exists, try to preselect its manifest
active_batch_id = st.session_state.get("active_batch_id")
if active_batch_id:
    for i, f in enumerate(manifests):
        if f["name"].startswith(active_batch_id) and f["name"].endswith("__manifest.json"):
            default_idx = i
            break

if manifests:
    labels = [f'{f["name"]} (modified {f.get("modifiedTime","")})' for f in manifests]
    choice = st.selectbox("배치(manifest) 선택", options=list(range(len(manifests))), format_func=lambda i: labels[i], index=default_idx)
    selected_manifest = manifests[choice]
else:
    st.info("manifest 파일이 없습니다. (배치 업로드를 먼저 진행하세요.)")

if selected_manifest:
    st.caption(f"선택된 manifest: `{selected_manifest['name']}`")
    try:
        manifest = read_json_file_by_id(drive, selected_manifest["id"])
    except Exception as e:
        st.error("manifest 읽기 실패")
        st.exception(e)
        st.stop()

    items = manifest.get("items", [])
    if not items:
        st.warning("manifest에 items가 없습니다. (업로드가 실패했거나 이전 버전일 수 있습니다.)")
    else:
        # Load meta per item
        rows = []
        terminal = True
        any_done = False
        done_targets = []  # (done_file name)
        for it in items:
            meta_name = it.get("meta_filename") or f'{it.get("job_id","")}.json'
            meta_obj = find_file_by_name(drive, folders["META"], meta_name)
            meta = None
            if meta_obj:
                try:
                    meta = read_json_file_by_id(drive, meta_obj["id"])
                except Exception:
                    meta = None

            status = (meta or {}).get("status", "unknown")
            progress = (meta or {}).get("progress", None)
            message = (meta or {}).get("message", "")
            done_file = (meta or {}).get("done_file", None)
            error_msg = (meta or {}).get("error", None)

            if status not in ("done", "error"):
                terminal = False
            if status == "done" and done_file:
                any_done = True
                done_targets.append(done_file)

            rows.append({
                "file": it.get("original_name") or it.get("inbox_name") or it.get("job_id"),
                "status": status,
                "progress": progress,
                "message": message,
                "done_file": done_file,
                "error": error_msg,
            })

        st.dataframe(rows, use_container_width=True)

        if auto_refresh and not terminal:
            try:
                from streamlit import st_autorefresh
                st_autorefresh(interval=refresh_sec * 1000, key="batch_poll")
            except Exception:
                pass

        st.markdown("#### 3) ZIP 다운로드")
        if not terminal:
            st.info("아직 모든 파일이 완료되지 않았습니다. (done/error가 될 때까지 기다려주세요)")
        elif not any_done:
            st.warning("완료(done)된 파일이 없어서 ZIP을 만들 수 없습니다.")
        else:
            # Build ZIP in memory
            if st.button("📦 ZIP 준비하기", type="secondary"):
                zip_buf = BytesIO()
                with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    # Add a report
                    report = {
                        "manifest": selected_manifest["name"],
                        "generated_at": now_seoul_iso(),
                        "items": rows,
                    }
                    zf.writestr("report.json", json.dumps(report, ensure_ascii=False, indent=2))

                    for done_name in done_targets:
                        # find in DONE folder by name
                        done_obj = find_file_by_name(drive, folders["DONE"], done_name)
                        if not done_obj:
                            # keep note in report
                            zf.writestr(f"missing/{done_name}.txt", "DONE folder에서 파일을 찾지 못했습니다.")
                            continue
                        data = download_file_bytes(drive, done_obj["id"])
                        zf.writestr(done_name, data)

                zip_bytes = zip_buf.getvalue()
                st.session_state["zip_bytes"] = zip_bytes
                batch_id = manifest.get("batch_id") or selected_manifest["name"].split("__manifest.json")[0]
                st.session_state["zip_name"] = f"{batch_id}.zip"
                st.success("ZIP 준비 완료! 아래 버튼으로 다운로드하세요.")

            if st.session_state.get("zip_bytes"):
                st.download_button(
                    label="⬇️ 결과 ZIP 다운로드",
                    data=st.session_state["zip_bytes"],
                    file_name=st.session_state.get("zip_name", "results.zip"),
                    mime="application/zip",
                    type="primary",
                )
                st.caption("다운로드 후 상단의 ‘🆕 새 작업 시작’ 버튼을 눌러 새 작업을 진행하세요.")
