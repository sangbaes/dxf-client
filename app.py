import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build

st.set_page_config(page_title="DXF Client – Drive Test")
st.title("DXF Client – Google Drive Connection Test")

# 1. Secrets 로드
try:
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    st.success("✅ Service Account credentials loaded")
except Exception as e:
    st.error("❌ Failed to load service account credentials")
    st.exception(e)
    st.stop()

# 2. Drive API 연결
try:
    drive = build("drive", "v3", credentials=creds)
    st.success("✅ Google Drive API connected")
except Exception as e:
    st.error("❌ Failed to connect to Google Drive API")
    st.exception(e)
    st.stop()

# 3. DXF_SHARED 폴더 검색
st.subheader("Searching for DXF_SHARED folder...")

query = (
    "name = 'DXF_SHARED' and "
    "mimeType = 'application/vnd.google-apps.folder' and "
    "trashed = false"
)

try:
    res = drive.files().list(
        q=query,
        fields="files(id, name)"
    ).execute()

    folders = res.get("files", [])

    if not folders:
        st.error("❌ DXF_SHARED folder not found")
        st.info("👉 Drive에 폴더가 존재하고 서비스 계정에 공유되었는지 확인하세요.")
    else:
        folder = folders[0]
        st.success("✅ DXF_SHARED folder found")
        st.code(f"Folder name: {folder['name']}\nFolder ID: {folder['id']}")
        st.info("👉 이 Folder ID를 다음 단계에서 고정값으로 사용합니다.")

except Exception as e:
    st.error("❌ Error while searching for folder")
    st.exception(e)