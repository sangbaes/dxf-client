import json
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build

st.set_page_config(page_title="DXF Client – Drive Test")
st.title("DXF Client – Google Drive Connection Test")

try:
    info = json.loads(st.secrets["SERVICE_ACCOUNT_JSON"])
    # 방어: 혹시 \\n 로 들어갔으면 실제 줄바꿈으로 복구
    info["private_key"] = info["private_key"].replace("\\n", "\n").strip()

    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    st.success("✅ Service Account credentials loaded")
except Exception as e:
    st.error("❌ Failed to load service account credentials")
    st.exception(e)
    st.stop()

drive = build("drive", "v3", credentials=creds)
st.success("✅ Google Drive API connected")

query = (
    "name = 'DXF_SHARED' and "
    "mimeType = 'application/vnd.google-apps.folder' and "
    "trashed = false"
)
res = drive.files().list(q=query, fields="files(id, name)").execute()
folders = res.get("files", [])

if not folders:
    st.error("❌ DXF_SHARED folder not found (공유 권한 확인 필요)")
else:
    st.success("✅ DXF_SHARED folder found")
    st.code(f"Folder ID: {folders[0]['id']}")