# --- FIXED UPLOADER BLOCK (drop-in) ---
# Place this in app.py, replacing the existing file_uploader block

uploaded_list = st.file_uploader(
    "DXF 파일 선택 (여러 개 가능)",
    type=["dxf"],
    accept_multiple_files=True,
    key=f"uploader_{st.session_state.get('uploader_key', 0)}",
)

# --- END FIX ---
