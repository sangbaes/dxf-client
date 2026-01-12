import json
from io import BytesIO
from collections import Counter

import streamlit as st
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload


# ✅ 당신 폴더 ID
META_FOLDER_ID = "1x2YCQTPOd5KC4tZdwfmX8zf7NZNO6Y_w"
DONE_FOLDER_ID = "1rC_1x1HAoJZ65YuGLDw8GikyBbqXWIJa"
META_ARCHIVE_FOLDER_ID = "1wo3yA5OpEeDIdPgRdQbznbZ9LUUNoNtK"

SCOPES = ["https://www.googleapis.com/auth/drive"]


@st.cache_resource(show_spinner=False)
def drive():
    cfg = st.secrets.get("drive_oauth") or st.secrets.get("DRIVE_OAUTH")
    if not cfg:
        st.error('Streamlit Secrets에 [drive_oauth]가 없습니다. Settings → Secrets에 OAuth 정보를 추가하세요.')
        st.stop()

    creds = Credentials(
        token=None,
        refresh_token=cfg["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def list_meta_files(limit=200):
    svc = drive()
    q = f"'{META_FOLDER_ID}' in parents and trashed=false"
    res = svc.files().list(
        q=q,
        fields="files(id,name,modifiedTime,createdTime,size)",
        orderBy="modifiedTime desc",
        pageSize=limit
    ).execute()
    files = [f for f in res.get("files", []) if f["name"].lower().endswith(".json")]
    return files


def download_json(file_id: str) -> dict:
    svc = drive()
    req = svc.files().get_media(fileId=file_id)
    buf = BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    raw = buf.getvalue().decode("utf-8", errors="replace")
    # 비표준 NULL 방어(혹시 남아있으면)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        fixed = raw.replace(":NULL", ":null").replace(": NULL", ": null")
        return json.loads(fixed)


def update_json(file_id: str, payload: dict):
    svc = drive()
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    media = MediaIoBaseUpload(BytesIO(data), mimetype="application/json", resumable=False)
    svc.files().update(fileId=file_id, media_body=media).execute()


def find_done_file(name: str):
    svc = drive()
    q = f"'{DONE_FOLDER_ID}' in parents and name='{name}' and trashed=false"
    res = svc.files().list(q=q, fields="files(id,name,size,modifiedTime)").execute()
    files = res.get("files", [])
    return files[0] if files else None


def move_file(file_id: str, from_folder_id: str, to_folder_id: str):
    svc = drive()
    svc.files().update(
        fileId=file_id,
        addParents=to_folder_id,
        removeParents=from_folder_id,
        fields="id, parents",
    ).execute()


def download_file_bytes(file_id: str) -> bytes:
    svc = drive()
    req = svc.files().get_media(fileId=file_id)
    buf = BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    return buf.getvalue()


st.set_page_config(page_title="DXF Monitor", layout="wide")
st.title("DXF Monitor")

# -----------------------
# Sidebar: auto refresh
# -----------------------
auto = st.sidebar.checkbox("자동 새로고침", True)
sec = st.sidebar.slider("주기(초)", 3, 30, 5)

if auto:
    try:
        from streamlit import st_autorefresh
        st_autorefresh(interval=sec * 1000, key="mon_auto")
    except Exception:
        pass

st.sidebar.caption("Folders")
st.sidebar.write(f"- META: `{META_FOLDER_ID}`")
st.sidebar.write(f"- DONE: `{DONE_FOLDER_ID}`")
st.sidebar.write(f"- META_ARCHIVE: `{META_ARCHIVE_FOLDER_ID}`")

# -----------------------
# Sidebar: META reset (ARCHIVE)
# -----------------------
st.sidebar.divider()
st.sidebar.subheader("관리")
reset_scope = st.sidebar.selectbox("리셋 대상", ["active만(queued/working/error)", "전체(done 포함)"])
confirm = st.sidebar.text_input('확인 문구 입력: RESET', value="")

if st.sidebar.button("META 리셋 (ARCHIVE로 이동)", type="primary", disabled=(confirm != "RESET")):
    # META 파일을 넉넉하게 가져옴
    meta_files = list_meta_files(limit=2000)
    moved = 0

    for f in meta_files:
        meta = download_json(f["id"])
        status = meta.get("status")

        if reset_scope.startswith("active") and status == "done":
            continue

        move_file(f["id"], META_FOLDER_ID, META_ARCHIVE_FOLDER_ID)
        moved += 1

    st.sidebar.success(f"완료: {moved}개 META를 ARCHIVE로 이동했습니다.")
    st.rerun()

# -----------------------
# Main: list jobs
# -----------------------
files = list_meta_files(limit=200)

rows = []
cache = {}  # job_id -> (meta_file_id, meta)

# 너무 많으면 느릴 수 있어 최근 80개만 우선
for f in files[:80]:
    meta = download_json(f["id"])
    job_id = meta.get("job_id") or f["name"].replace(".json", "")
    cache[job_id] = (f["id"], meta)
    rows.append({
        "job_id": job_id,
        "status": meta.get("status"),
        "progress": int(meta.get("progress") or 0),
        "updated_at": meta.get("updated_at"),
        "original_name": meta.get("original_name"),
        "message": meta.get("message"),
    })

cnt = Counter([r["status"] for r in rows])
c1, c2, c3, c4 = st.columns(4)
c1.metric("queued", cnt.get("queued", 0))
c2.metric("working", cnt.get("working", 0))
c3.metric("done", cnt.get("done", 0))
c4.metric("error", cnt.get("error", 0))

st.divider()
st.subheader("Jobs (최근 80개)")
st.dataframe(rows, width="stretch", hide_index=True)

st.subheader("Job detail")
job_ids = [r["job_id"] for r in rows]
selected = st.selectbox("job_id 선택", job_ids) if job_ids else None

if selected:
    meta_file_id, meta = cache[selected]

    colA, colB = st.columns([1, 1])
    with colA:
        st.write("### Status")
        st.write(f"**status:** `{meta.get('status')}`")
        st.write(f"**progress:** `{meta.get('progress')}`")
        st.write(f"**updated_at:** `{meta.get('updated_at')}`")
        st.write(f"**message:** {meta.get('message')}")
        st.progress(min(max(int(meta.get("progress") or 0), 0), 100) / 100)

    with colB:
        st.write("### Logs / Error")
        if meta.get("error"):
            st.error("error")
            st.code(meta.get("error"))
        if meta.get("translator_log_tail"):
            st.caption("translator_log_tail")
            st.code(meta.get("translator_log_tail"))

    st.write("### META JSON")
    st.json(meta)

    # (선택) 에러/스턱 상황에서 재시도 버튼
    if meta.get("status") in ("error", "working"):
        st.caption("운영 버튼 (선택)")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("queued로 되돌리기(재시도)", type="primary"):
                meta["status"] = "queued"
                meta["progress"] = 0
                meta["message"] = "Re-queued by monitor."
                meta["error"] = None
                meta["updated_at"] = None  # 워커가 다시 세팅
                update_json(meta_file_id, meta)
                st.success("queued로 되돌렸습니다. 잠시 후 워커가 다시 잡습니다.")
                st.rerun()
        with col2:
            if st.button("working → error로 강제 종료 표시"):
                meta["status"] = "error"
                meta["progress"] = 100
                meta["message"] = "Manually marked as error."
                meta["error"] = meta.get("error") or "Manually marked."
                update_json(meta_file_id, meta)
                st.success("error로 표시했습니다.")
                st.rerun()

    # DONE 다운로드
    if meta.get("status") == "done" and meta.get("done_file"):
        done_obj = find_done_file(meta["done_file"])
        if done_obj:
            data = download_file_bytes(done_obj["id"])
            st.download_button(
                "DONE 결과 다운로드",
                data=data,
                file_name=meta["done_file"],
                mime="application/dxf",
                type="primary",
            )
        else:
            st.warning("DONE 폴더에서 결과 파일을 아직 찾지 못했습니다.")

st.divider()
st.subheader("완료된 결과 파일 (최근 30개)")

@st.cache_data(show_spinner=False, ttl=60*10)
def _cached_download_file_bytes(file_id: str) -> bytes:
    return download_file_bytes(file_id)

done_candidates = []
for r in rows:
    if r.get("status") != "done":
        continue
    meta_file_id, meta = cache.get(r["job_id"], (None, None))
    if not meta:
        continue
    done_name = meta.get("done_file")
    if not done_name:
        continue
    done_obj = find_done_file(done_name)
    if not done_obj:
        continue
    done_candidates.append({
        "job_id": r["job_id"],
        "original": meta.get("original_name") or r.get("names"),
        "done_file": done_name,
        "done_file_id": done_obj["id"],
        "updated_at": meta.get("updated_at"),
        "message": meta.get("message"),
    })

# 최신순 정렬(문자열 ISO 형식 가정)
done_candidates.sort(key=lambda x: (x.get("updated_at") or ""), reverse=True)
done_candidates = done_candidates[:30]

if not done_candidates:
    st.info("DONE 상태의 작업이 아직 없거나, DONE 폴더에서 결과 파일을 찾지 못했습니다.")
else:
    for item in done_candidates:
        c1, c2, c3, c4 = st.columns([3, 3, 2, 2])
        with c1:
            st.write(f"**{item['done_file']}**")
            if item.get("original"):
                st.caption(f"원본: {item['original']}")
        with c2:
            st.write(f"`job_id`: {item['job_id']}")
            if item.get("updated_at"):
                st.caption(f"updated_at: {item['updated_at']}")
            if item.get("message"):
                st.caption(item["message"])
        with c3:
            # 버튼 클릭 시에만 다운로드 바이트 준비
            if st.button("파일 준비", key=f"prep_{item['done_file_id']}"):
                st.session_state[f"data_{item['done_file_id']}"] = _cached_download_file_bytes(item["done_file_id"])
        with c4:
            data_key = f"data_{item['done_file_id']}"
            if data_key in st.session_state:
                st.download_button(
                    "다운로드",
                    data=st.session_state[data_key],
                    file_name=item["done_file"],
                    mime="application/dxf",
                    key=f"dl_{item['done_file_id']}",
                    type="primary",
                )
            else:
                st.caption("준비 필요")
        st.divider()
