import streamlit as st

st.set_page_config(
    page_title="DXF Client",
    layout="centered"
)

st.title("DXF Translation Client")

st.info(
    """
    이 앱은 DXF 파일 업로드 후
    로컬 번역 워커(MacBook Pro)에서 처리하고
    완료되면 다운로드를 제공하는 클라이언트입니다.

    현재는 초기 설정 단계입니다.
    """
)

st.subheader("Status")
st.write("🟡 준비 중 (Drive 연동 예정)")

st.divider()

st.subheader("Next steps")
st.markdown(
    """
    - Google Drive 연동
    - DXF 파일 업로드
    - 작업 상태 확인
    - 번역 완료 파일 다운로드
    """
)

st.caption("DXF Client · Streamlit Cloud")