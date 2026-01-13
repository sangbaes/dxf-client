import base64
import json
import time
import uuid
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from io import BytesIO
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
    # Remove spaces and keep only safe characters
    safe = original_name.replace(" ", "")
    safe = "".join(c for c in safe if c.isalnum() or c in ("-", "_", "."))
    safe = safe[:40] if safe else "file"
    return f"{ts}_{short}_{safe}"



def drive_execute(req, retries: int = 5, base_sleep: float = 0.6):
    """Drive API ìš”ì²­ì„ ë„¤íŠ¸ì›Œí¬ í”ë“¤ë¦¼ì—ë„ ìµœëŒ€í•œ ê²¬ë””ë„ë¡ ìž¬ì‹œë„ ì‹¤í–‰."""
    last_err = None
    for i in range(retries + 1):
        try:
            # googleapiclient ìžì²´ ìž¬ì‹œë„ë„ ìžˆì§€ë§Œ, SSL read/connection resetì€ ì§ì ‘ ê°ì‹¸ì£¼ëŠ” ê²Œ ë” ì•ˆì •ì ìž„
            return req.execute(num_retries=1)
        except (HttpError, OSError, ssl.SSLError, socket.timeout) as e:
            last_err = e
            if i >= retries:
                raise
            time.sleep(base_sleep * (2 ** i))
    raise last_err


def load_service_account_info():
    # âœ… Base64 ë°©ì‹ (Streamlit Secrets: SERVICE_ACCOUNT_B64)
    if "SERVICE_ACCOUNT_B64" not in st.secrets:
        raise RuntimeError("Streamlit Secretsì— SERVICE_ACCOUNT_B64ê°€ ì—†ìŠµë‹ˆë‹¤.")

    raw = base64.b64decode(st.secrets["SERVICE_ACCOUNT_B64"].encode("ascii"))
    info = json.loads(raw.decode("utf-8"))

    # ë°©ì–´: í˜¹ì‹œ \\në¡œ ì €ìž¥ëœ ê²½ìš° ì‹¤ì œ ì¤„ë°”ê¿ˆìœ¼ë¡œ ë³µêµ¬
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n").strip()

    return info

    if "SERVICE_ACCOUNT_JSON" in st.secrets:
        info = json.loads(st.secrets["SERVICE_ACCOUNT_JSON"])
        if "private_key" in info and isinstance(info["private_key"], str):
            info["private_key"] = info["private_key"].replace("\\n", "\n").strip()
        return info

    raise RuntimeError(
        "Streamlit Secretsì— gcp_service_account ë˜ëŠ” SERVICE_ACCOUNT_JSONì´ ì—†ìŠµë‹ˆë‹¤."
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
    # access_tokenì´ í•„ìš”í•  ë•Œ ìžë™ ê°±ì‹ 
    creds.refresh(Request())

    # Streamlit Cloudì—ì„œ ê°„í—ì ìœ¼ë¡œ ë°œìƒí•˜ëŠ” SSL/ë„¤íŠ¸ì›Œí¬ read ë¬¸ì œë¥¼ ì™„í™”í•˜ê¸° ìœ„í•´
    # - httplib2 timeout ì§€ì •
    # - AuthorizedHttp ì‚¬ìš©
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
        # Drive APIê°€ ì¼ì‹œì ìœ¼ë¡œ í”ë“¤ë¦´ ë•Œ ì•±ì´ ì£½ì§€ ì•Šê²Œ ë°©ì–´
        st.warning(
            "Google Drive ì¡°íšŒê°€ ì¼ì‹œì ìœ¼ë¡œ ì‹¤íŒ¨í–ˆìŠµë‹ˆë‹¤. ìž ì‹œ í›„ ìžë™ìœ¼ë¡œ ë‹¤ì‹œ ì‹œë„í•©ë‹ˆë‹¤.\n"
            f"ì›ì¸: {type(e).__name__}"
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

with st.expander("ì„¤ëª…", expanded=False):
    st.markdown(
        """
- ì´ ì•±ì€ **Google Drive ê³µìœ í´ë”ì— DXFë¥¼ ì—…ë¡œë“œ**í•˜ê³ ,
- MacBook Pro ë¡œì»¬ ì›Œì»¤ê°€ ë²ˆì—­ í›„ ê²°ê³¼ë¥¼ `DONE/`ì— ì˜¬ë¦¬ë©´,
- ì•±ì´ ì™„ë£Œë¥¼ ê°ì§€í•´ **ë‹¤ìš´ë¡œë“œ ë²„íŠ¼**ì„ ì œê³µí•©ë‹ˆë‹¤.
        """
    )

# Drive connection
try:
    drive = get_drive_service()
    folders = get_subfolder_ids(drive)
except Exception as e:
    st.error("Google Drive ì—°ê²°/í´ë” ì´ˆê¸°í™”ì— ì‹¤íŒ¨í–ˆìŠµë‹ˆë‹¤. Secrets ë˜ëŠ” í´ë” ê³µìœ  ê¶Œí•œì„ í™•ì¸í•˜ì„¸ìš”.")
    st.exception(e)
    st.stop()

st.success("âœ… Google Drive ì—°ê²°ë¨")
st.caption(f"DXF_SHARED_FOLDER_ID = {DXF_SHARED_FOLDER_ID}")

# Sidebar controls
st.sidebar.header("ì˜µì…˜")
auto_refresh = st.sidebar.checkbox("ìƒíƒœ ìžë™ ìƒˆë¡œê³ ì¹¨", value=True)
refresh_sec = st.sidebar.slider("ìƒˆë¡œê³ ì¹¨ ì£¼ê¸°(ì´ˆ)", 3, 30, 5)

st.sidebar.divider()
st.sidebar.caption("í´ë”")
for k in SUBFOLDERS:
    st.sidebar.write(f"- {k}: `{folders[k]}`")

# Upload section
st.subheader("1) DXF ì—…ë¡œë“œ (Batch)")
uploaded_list = st.file_uploader(
    "DXF íŒŒì¼ ì„ íƒ (ì—¬ëŸ¬ ê°œ ê°€ëŠ¥)",
    type=["dxf"],
    accept_multiple_files=True
)

def _make_batch_id() -> str:
    # stable, time-sortable prefix + short uuid
    # example: 20260112_173210_ab12cd34
    ts = now_seoul_iso().replace(":", "").replace("-", "").replace("+0900", "").replace("T", "_")
    # ts like 20260112_173210.123+09:00 depending on your formatter; keep it simple:
    ts = ts.split(".")[0].replace("+09:00", "").replace("+0900", "")
    return f"{ts}_{uuid.uuid4().hex[:8]}"

def _safe_name(name: str) -> str:
    # Prevent weird path-ish names and remove spaces
    safe = Path(name).name
    # Replace spaces with underscores
    safe = safe.replace(" ", "_")
    # Remove any remaining problematic characters
    safe = "".join(c for c in safe if c.isalnum() or c in ("-", "_", "."))
    return safe if safe else "file.dxf"

if uploaded_list:
    total_files = len(uploaded_list)
    total_size = sum(u.size for u in uploaded_list)

    st.write(f"ì„ íƒëœ íŒŒì¼: **{total_files}ê°œ** / ì´ í¬ê¸°: **{total_size/1024/1024:.1f} MB**")
    too_big = [u for u in uploaded_list if u.size > MAX_FILE_BYTES]

    if too_big:
        st.error(
            "ì•„ëž˜ íŒŒì¼ì´ ë„ˆë¬´ í½ë‹ˆë‹¤. "
            f"{MAX_FILE_MB}MB ì´í•˜ë§Œ ì—…ë¡œë“œí•  ìˆ˜ ìžˆìŠµë‹ˆë‹¤:\n- "
            + "\n- ".join([f"{u.name} ({u.size/1024/1024:.1f}MB)" for u in too_big])
        )
    else:
        # Batch upload button
        if st.button("INBOXë¡œ ì¼ê´„ ì—…ë¡œë“œ", type="primary"):
            st.session_state.pop("upload_progress", None)

            batch_id = _make_batch_id()
            created_at = now_seoul_iso()

            manifest_filename = f"{batch_id}__manifest.json"
            manifest_payload = {
                "batch_id": batch_id,
                "status": "uploading",  # uploading | queued | done | error
                "created_at": created_at,
                "updated_at": created_at,
                "total": total_files,
                "items": [],  # filled below
                "message": "Uploading files to INBOX and writing META items."
            }

            progress = st.progress(0)
            status_box = st.empty()

            ok_count = 0
            errors = []

            with st.spinner("ì—…ë¡œë“œ ì¤‘..."):
                for idx, uploaded in enumerate(uploaded_list, 1):
                    try:
                        safe_orig = _safe_name(uploaded.name)
                        file_bytes = uploaded.getvalue()

                        # Keep current behavior: job_id derived from filename (unique enough in practice)
                        # BUT add batch_id to correlate items.
                        job_id = make_job_id(safe_orig)

                        inbox_name = f"{job_id}__{safe_orig}"
                        meta_filename = f"{job_id}.json"

                        meta_payload = {
                            "batch_id": batch_id,           # NEW (safe)
                            "job_id": job_id,
                            "original_name": safe_orig,
                            "inbox_name": inbox_name,
                            "status": "queued",             # queued | working | done | error
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
                            file_bytes,
                            mime="application/dxf"
                        )
                        meta_payload["inbox_file_id"] = resp.get("id")
                        meta_payload["progress"] = 5
                        meta_payload["updated_at"] = now_seoul_iso()

                        # 2) Upsert META (per-file)
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
                    status_box.write(f"ì—…ë¡œë“œ ì§„í–‰: {idx}/{total_files} (ì„±ê³µ {ok_count} / ì‹¤íŒ¨ {len(errors)})")

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
                st.error("âŒ manifest ì €ìž¥ ì‹¤íŒ¨")
                st.exception(e)

            # Store batch context in session for the monitoring UI
            st.session_state["active_batch_id"] = batch_id
            st.session_state["active_job_ids"] = [it["job_id"] for it in manifest_payload["items"]]

            if errors:
                st.warning(f"âš ï¸ ì¼ë¶€ ì—…ë¡œë“œ ì‹¤íŒ¨: {len(errors)}ê°œ")
                st.json(errors)
            st.success("âœ… ë°°ì¹˜ ì—…ë¡œë“œ ì™„ë£Œ")
            st.code(f"batch_id: {batch_id}")


st.subheader("2) ìž‘ì—… ìƒíƒœ / ë‹¤ìš´ë¡œë“œ")

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
    job_id = st.selectbox("ìµœê·¼ ìž‘ì—… ì„ íƒ", recent_ids, index=default_index)
else:
    st.info("META í´ë”ì— ìž‘ì—…ì´ ì•„ì§ ì—†ìŠµë‹ˆë‹¤. ë¨¼ì € ì—…ë¡œë“œí•˜ì„¸ìš”.")

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
    manual_refresh = st.button("ìƒíƒœ ìƒˆë¡œê³ ì¹¨")
with col_b:
    st.caption("ìžë™ ìƒˆë¡œê³ ì¹¨ì´ ì•ˆ ë˜ë©´ ë²„íŠ¼ì„ ì‚¬ìš©í•˜ì„¸ìš”.")

if job_id:
    meta_name = f"{job_id}.json"
    meta = None
    try:
        meta = read_json_file_by_name(drive, folders["META"], meta_name)
    except Exception as e:
        st.error("META ì½ê¸° ì‹¤íŒ¨")
        st.exception(e)

    if meta:
        st.write(f"**status:** `{meta.get('status')}`")
        st.write(f"**updated_at:** `{meta.get('updated_at')}`")
        st.write(f"**message:** {meta.get('message')}")
        prog = int(meta.get("progress", 0) or 0)
        st.progress(min(max(prog, 0), 100) / 100.0)

        if meta.get("status") == "error":
            st.error("ìž‘ì—… ì‹¤íŒ¨")
            if meta.get("error"):
                st.code(meta.get("error"))

        if meta.get("status") == "done":
            done_file = meta.get("done_file")
            if not done_file:
                st.warning("done ìƒíƒœì§€ë§Œ done_file ì •ë³´ê°€ METAì— ì—†ìŠµë‹ˆë‹¤.")
            else:
                st.success("âœ… ë²ˆì—­ ì™„ë£Œ")
                st.write(f"ê²°ê³¼ íŒŒì¼: `{done_file}`")

                done_obj = find_file_in_folder_by_name(drive, folders["DONE"], done_file)
                if not done_obj:
                    st.warning("DONE í´ë”ì—ì„œ ê²°ê³¼ íŒŒì¼ì„ ì•„ì§ ì°¾ì§€ ëª»í–ˆìŠµë‹ˆë‹¤. ìž ì‹œ í›„ ë‹¤ì‹œ ì‹œë„í•˜ì„¸ìš”.")
                else:
                    # Download through API and offer download button
                    try:
                        with st.spinner("ê²°ê³¼ íŒŒì¼ ë‹¤ìš´ë¡œë“œ ì¤€ë¹„ ì¤‘... (íŒŒì¼ì´ í¬ë©´ ì‹œê°„ì´ ê±¸ë¦´ ìˆ˜ ìžˆìŠµë‹ˆë‹¤)"):
                            data = download_file_bytes(drive, done_obj["id"])
                        st.download_button(
                            label="ê²°ê³¼ DXF ë‹¤ìš´ë¡œë“œ",
                            data=data,
                            file_name=done_file,
                            mime="application/dxf",
                            type="primary",
                        )
                    except Exception as e:
                        st.error("ê²°ê³¼ íŒŒì¼ ë‹¤ìš´ë¡œë“œ ì¤€ë¹„ ì‹¤íŒ¨")
                        st.exception(e)

    else:
        st.info("í•´ë‹¹ jobì˜ META íŒŒì¼ì„ ì•„ì§ ì°¾ì§€ ëª»í–ˆìŠµë‹ˆë‹¤.")