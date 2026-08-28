from pathlib import Path
import json, re, textwrap

src_path = Path("/mnt/data/Pasted text(20260828-130406).txt")
nb = json.loads(src_path.read_text(encoding="utf-8"))

# Extract the main code cell from the uploaded notebook JSON.
code_cells = ["".join(c.get("source", [])) for c in nb.get("cells", []) if c.get("cell_type") == "code"]
main_src = max(code_cells, key=len)

# Keep the original dashboard/processing logic, but make it runnable as a Streamlit .py app.
app = main_src

# 1) Imports: replace IPython-only display utilities with Streamlit.
app = app.replace(
    "from IPython.display import HTML, display, clear_output\n",
    "import os\nimport streamlit as st\n"
)

# 2) Replace eager model initialization with Streamlit cache.
old_model = '''# ------------------------------------------------------------------------------
# 2. KHỞI TẠO MÔ HÌNH AI
# ------------------------------------------------------------------------------
print("Đang tải mô hình Trí tuệ nhân tạo (RoBERTa)...")
sentiment_analyzer = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
clear_output()
'''
new_model = '''# ------------------------------------------------------------------------------
# 2. KHỞI TẠO MÔ HÌNH AI
# ------------------------------------------------------------------------------
@st.cache_resource(show_spinner="Đang tải mô hình AI RoBERTa...")
def load_sentiment_analyzer():
    return pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-roberta-base-sentiment-latest"
    )

sentiment_analyzer = None
'''
app = app.replace(old_model, new_model)

# 3) Remove hard-coded OCI credentials and read them from Streamlit Secrets/env.
cred_pattern = re.compile(
    r"# Sử dụng đúng chuỗi username do OCI cung cấp\n"
    r"SASL_USERNAME = .*?\n"
    r"OCI_AUTH_TOKEN = .*?\n",
    flags=re.S,
)
cred_replacement = '''# Không hard-code thông tin xác thực trong GitHub.
# Trên Streamlit Cloud: App -> Settings -> Secrets, khai báo:
# OCI_SASL_USERNAME = "..."
# OCI_AUTH_TOKEN = "..."
def get_secret(name, default=""):
    try:
        return st.secrets.get(name, os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)

SASL_USERNAME = get_secret("OCI_SASL_USERNAME")
OCI_AUTH_TOKEN = get_secret("OCI_AUTH_TOKEN")
'''
app = cred_pattern.sub(cred_replacement, app, count=1)

# 4) Let dashboard show configurable duration.
app = app.replace(
    "def generate_full_dashboard(rows, consumed_count, started_at):",
    "def generate_full_dashboard(rows, consumed_count, started_at, duration_seconds=DURATION_SECONDS):"
)
app = app.replace(
    '{elapsed:.1f}s / 1800s',
    '{elapsed:.1f}s / {duration_seconds:.0f}s'
)

# 5) Convert the notebook display/update loop to Streamlit placeholders.
app = app.replace(
    "def run_stream_demo():\n    global use_fallback",
    "def run_stream_demo(duration_seconds=DURATION_SECONDS):\n    global use_fallback, sentiment_analyzer"
)
app = app.replace(
    "    use_fallback = False\n\n    stop_event = threading.Event()",
    '''    use_fallback = False

    if not SASL_USERNAME or not OCI_AUTH_TOKEN:
        st.error("Thiếu OCI_SASL_USERNAME hoặc OCI_AUTH_TOKEN trong Streamlit Secrets.")
        return pd.DataFrame()

    if sentiment_analyzer is None:
        sentiment_analyzer = load_sentiment_analyzer()

    stop_event = threading.Event()'''
)
app = app.replace(
    '    dash_handle = display(HTML("<div style=\'font-family: sans-serif; color: #666;\'>Hệ thống đang khởi tạo luồng dữ liệu tốc độ cao...</div>"), display_id="live_monitor")',
    '''    dash_handle = st.empty()
    dash_handle.markdown(
        "<div style='font-family: sans-serif; color: #666;'>Hệ thống đang khởi tạo luồng dữ liệu tốc độ cao...</div>",
        unsafe_allow_html=True
    )'''
)
app = app.replace(
    "while time.monotonic() - started_at < DURATION_SECONDS:",
    "while time.monotonic() - started_at < duration_seconds:"
)
app = app.replace(
    "html_content = generate_full_dashboard(rows, consumed_count, started_at)\n                dash_handle.update(HTML(html_content))",
    "html_content = generate_full_dashboard(rows, consumed_count, started_at, duration_seconds)\n                dash_handle.markdown(html_content, unsafe_allow_html=True)"
)
app = app.replace(
    "    dash_handle.update(HTML(generate_full_dashboard(rows, consumed_count, started_at)))",
    "    dash_handle.markdown(generate_full_dashboard(rows, consumed_count, started_at, duration_seconds), unsafe_allow_html=True)"
)
app = app.replace(
    '    print("\\nQuá trình streaming tốc độ cao đã hoàn tất thành công.")',
    '    st.success("Quá trình streaming tốc độ cao đã hoàn tất thành công.")'
)

# 6) Replace notebook auto-run with Streamlit page UI.
app = app.replace(
    "results_df = run_stream_demo()",
    '''# ------------------------------------------------------------------------------
# 8. GIAO DIỆN STREAMLIT
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="Amazon Fashion Streaming Sentiment",
    page_icon="📊",
    layout="wide"
)

st.title("Ứng dụng Big Data Streaming phân tích độ hài lòng khách hàng")
st.caption("Amazon Fashion Reviews + OCI Streaming + RoBERTa Sentiment Analysis")

with st.sidebar:
    st.header("Thiết lập")
    duration_seconds = st.number_input(
        "Thời gian chạy (giây)",
        min_value=10,
        max_value=1800,
        value=1800,
        step=10
    )
    st.write(f"Topic: `{TOPIC}`")
    st.write(f"Bootstrap server: `{BOOTSTRAP_SERVERS}`")

if not SASL_USERNAME or not OCI_AUTH_TOKEN:
    st.warning(
        "Chưa cấu hình OCI credentials. Vào Streamlit Cloud → App → Settings → "
        "Secrets và thêm OCI_SASL_USERNAME, OCI_AUTH_TOKEN."
    )

if st.button("Bắt đầu chạy Streaming", type="primary"):
    results_df = run_stream_demo(duration_seconds=float(duration_seconds))

    if not results_df.empty:
        st.subheader("Dữ liệu kết quả")
        st.dataframe(results_df, use_container_width=True)

        csv_data = results_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "Tải kết quả CSV",
            data=csv_data,
            file_name="streaming_sentiment_results.csv",
            mime="text/csv"
        )
'''
)

# Add a small header note.
header
