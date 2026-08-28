# app.py
# Bản sửa lỗi giao diện cho practice8.
# Thay đổi chính so với bản cũ:
#   1. st.html()/components.html() thay cho st.markdown(unsafe_allow_html=True)
#   2. html.escape() cho nội dung review
#   3. table-layout: fixed + object-fit: contain -> hết giật layout
#   4. Cache biểu đồ 3D theo counts, chu kỳ render 2s
#   5. matplotlib backend Agg (an toàn với thread)
#   6. Banner trạng thái phản ánh đúng lỗi producer / fallback
#   7. set_page_config là lệnh Streamlit đầu tiên

import streamlit as st

# ------------------------------------------------------------------------------
# 1. PAGE CONFIG - PHẢI LÀ LỆNH STREAMLIT ĐẦU TIÊN
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="Amazon Fashion Streaming Sentiment",
    page_icon="📊",
    layout="wide",
)

import base64
import gzip
import html as html_lib
import io
import json
import os
import queue
import threading
import time
import uuid

import certifi
import matplotlib

matplotlib.use("Agg")  # bắt buộc: producer chạy trong thread riêng

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import streamlit.components.v1 as components
from confluent_kafka import Consumer, Producer
from transformers import pipeline

pd.set_option("display.max_columns", 40)


# ------------------------------------------------------------------------------
# 2. KHỞI TẠO MÔ HÌNH AI
# ------------------------------------------------------------------------------
@st.cache_resource(show_spinner="Đang tải mô hình AI RoBERTa...")
def load_sentiment_analyzer():
    return pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-roberta-base-sentiment-latest",
    )


sentiment_analyzer = None


# ------------------------------------------------------------------------------
# 3. CẤU HÌNH OCI & DATASET
# ------------------------------------------------------------------------------
DATA_URL = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_v2/categoryFiles/AMAZON_FASHION.json.gz"
BOOTSTRAP_SERVERS = "cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092"
DURATION_SECONDS = 300  # mặc định 5 phút; 1800s dễ bị Streamlit Cloud ngắt websocket
TOPIC = "DemoStreamingFashion"

RENDER_INTERVAL = 2.0  # giây - 1.0s làm nháy màn hình vì phải encode lại ảnh PNG
MAX_TITLE_CHARS = 180
TABLE_ROWS = 7


def get_secret(name, default=""):
    try:
        return st.secrets.get(name, os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)


SASL_USERNAME = get_secret("OCI_SASL_USERNAME")
OCI_AUTH_TOKEN = get_secret("OCI_AUTH_TOKEN")

RUN_ID = uuid.uuid4().hex[:8]
GROUP_ID = f"fashion_stream_{RUN_ID}"

COMMON_KAFKA_CONF = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": SASL_USERNAME,
    "sasl.password": OCI_AUTH_TOKEN,
    "ssl.ca.location": certifi.where(),
}

PRODUCER_CONF = {
    **COMMON_KAFKA_CONF,
    "client.id": f"prod_{RUN_ID}",
    "linger.ms": 10,
    "acks": "1",
}
CONSUMER_CONF = {
    **COMMON_KAFKA_CONF,
    "client.id": f"cons_{RUN_ID}",
    "group.id": GROUP_ID,
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
}


# ------------------------------------------------------------------------------
# 4. PRODUCER
# ------------------------------------------------------------------------------
producer_lock = threading.Lock()
producer_stats = {
    "generated": 0,
    "delivered": 0,
    "failed": 0,
    "error": "",
}
local_queue = queue.Queue()
use_fallback = False


def delivery_report(err, msg):
    with producer_lock:
        if err:
            producer_stats["failed"] += 1
            producer_stats["error"] = str(err)
        else:
            producer_stats["delivered"] += 1


def classify(amazon_rating, ai_label):
    """Gộp rating Amazon với nhãn AI thành 1 trong 5 mức cảm xúc."""
    if amazon_rating <= 2.0 and "pos" in ai_label:
        effective = "negative"
    elif amazon_rating >= 4.0 and "neg" in ai_label:
        effective = "negative"
    elif "pos" in ai_label:
        effective = "positive"
    elif "neg" in ai_label:
        effective = "negative"
    else:
        effective = "neutral"

    if effective == "positive":
        if amazon_rating >= 4.5:
            return "Rất tích cực", "#052c11", "#a3cfbb"
        return "Tích cực", "#0f5132", "#d1e7dd"
    if effective == "negative":
        if amazon_rating <= 1.5:
            return "Rất tiêu cực", "#58151c", "#f1aeb5"
        return "Tiêu cực", "#842029", "#f8d7da"
    return "Trung lập", "#41464b", "#e2e3e5"


def file_streaming_producer_worker(producer, stop_event):
    global use_fallback
    try:
        response = requests.get(DATA_URL, stream=True, timeout=30)
        response.raise_for_status()
        with gzip.GzipFile(fileobj=response.raw) as gz:
            for line in gz:
                if stop_event.is_set():
                    break
                if not line.strip():
                    continue
                try:
                    raw_record = json.loads(line.decode("utf-8"))
                except Exception:
                    continue

                amazon_rating = float(raw_record.get("overall", 0.0))
                title = str(raw_record.get("reviewText", "")).strip()
                if not title:
                    title = "Không có tiêu đề"

                ai_result = sentiment_analyzer(title[:500])[0]
                emotion_text, text_color, bg_color = classify(
                    amazon_rating, ai_result["label"].lower()
                )

                event = {
                    "run_id": RUN_ID,
                    "amazon_rating": amazon_rating,
                    "title": title,
                    "emotion": emotion_text,
                    "t_color": text_color,
                    "b_color": bg_color,
                }
                payload = json.dumps(event).encode("utf-8")
                local_queue.put(payload)

                if not use_fallback:
                    producer.produce(TOPIC, value=payload, on_delivery=delivery_report)
                    producer.poll(0)

                with producer_lock:
                    producer_stats["generated"] += 1
    except Exception as exc:
        with producer_lock:
            producer_stats["error"] = f"Lỗi nguồn dữ liệu: {exc}"

    if not use_fallback:
        try:
            producer.flush(5)
        except Exception:
            pass


# ------------------------------------------------------------------------------
# 5. BIỂU ĐỒ 3D (có cache để không encode lại PNG mỗi lần render)
# ------------------------------------------------------------------------------
CATEGORIES = ["Rất tích cực", "Tích cực", "Trung lập", "Tiêu cực", "Rất tiêu cực"]
BAR_COLORS = ["#2b8a3e", "#51cf66", "#ced4da", "#ff6b6b", "#c92a2a"]

_chart_cache = {"key": None, "src": ""}


def count_emotions(rows):
    counts = {cat: 0 for cat in CATEGORIES}
    for row in rows:
        emo = row.get("emotion")
        if emo in counts:
            counts[emo] += 1
    return counts


def build_3d_chart(counts):
    """Trả về data URI. Chỉ vẽ lại khi counts thay đổi."""
    key = tuple(counts[c] for c in CATEGORIES)
    if _chart_cache["key"] == key and _chart_cache["src"]:
        return _chart_cache["src"]

    y_vals = list(key)

    fig = plt.figure(figsize=(6.2, 4.2), facecolor="white")
    ax = fig.add_subplot(projection="3d")

    x = np.arange(len(CATEGORIES))
    zeros = np.zeros(len(CATEGORIES))
    dx = np.full(len(CATEGORIES), 0.4)
    dy = np.full(len(CATEGORIES), 0.4)

    ax.bar3d(
        x - 0.2, zeros, zeros, dx, dy, y_vals,
        color=BAR_COLORS, shade=True, edgecolor="none", alpha=0.92,
    )
    ax.view_init(elev=28, azim=-55)
    ax.set_xticks(x)
    ax.set_xticklabels(
        CATEGORIES, fontsize=9, rotation=25, color="#212529", ha="right"
    )
    ax.set_yticks([])
    ax.set_zlabel("Số lượng", fontsize=9, fontweight="bold", color="#212529")
    # tránh trục Z co giãn kỳ lạ khi tất cả bằng 0
    ax.set_zlim(0, max(max(y_vals), 1) * 1.1)
    ax.set_title(
        "Biểu đồ 3D Phân phối cảm xúc",
        fontsize=11, fontweight="bold", color="#1d3557", pad=12,
    )
    for axis in (ax.xaxis, ax.yaxis, ax.zaxis):
        axis.set_pane_color((0.95, 0.95, 0.95, 0.8))

    buf = io.BytesIO()
    # bbox_inches='tight' giữ nhãn không bị cắt; kích thước ảnh có đổi nhưng
    # CSS object-fit: contain phía dưới đã khóa chiều cao nên layout không nhảy.
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
    buf.seek(0)

    src = "data:image/png;base64," + base64.b64encode(buf.read()).decode("utf-8")
    _chart_cache["key"] = key
    _chart_cache["src"] = src
    return src


# ------------------------------------------------------------------------------
# 6. DASHBOARD HTML
# ------------------------------------------------------------------------------
class MockMessage:
    def __init__(self, val):
        self._val = val

    def value(self):
        return self._val

    def error(self):
        return None


DASHBOARD_CSS = """
<style>
.bd-wrap { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  padding: 22px; background: linear-gradient(135deg, #0d6efd, #0a58ca);
  border-radius: 14px; box-shadow: 0 6px 16px rgba(0,0,0,.15); color: #fff; }
.bd-title { margin: 0 0 15px; color: #fff; font-size: 1.45em; font-weight: 700;
  text-transform: uppercase; letter-spacing: .5px;
  border-bottom: 2px solid rgba(255,255,255,.3); padding-bottom: 12px; }
.bd-banner { padding: 11px 18px; border-radius: 8px; font-weight: 600;
  font-size: .93em; margin-bottom: 18px; }
.bd-ok   { background: #d1e7dd; color: #0f5132; border: 1px solid #badbcc; }
.bd-warn { background: #fff3cd; color: #664d03; border: 1px solid #ffe69c; }
.bd-err  { background: #f8d7da; color: #842029; border: 1px solid #f5c2c7; }
.bd-stats { display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 18px; }
.bd-card { flex: 1 1 150px; background: #fff; padding: 14px; border-radius: 8px;
  border: 1px solid #dee2e6; text-align: center; color: #212529; }
.bd-card-t { font-size: .74em; color: #6c757d; text-transform: uppercase;
  font-weight: 600; letter-spacing: 1px; margin-bottom: 5px; }
.bd-card-v { font-size: 1.45em; font-weight: 700; color: #212529; }
.bd-blue { color: #0d6efd; } .bd-green { color: #198754; }
.bd-grid { display: flex; flex-wrap: wrap; gap: 18px; align-items: stretch; }
.bd-col { background: #fff; padding: 16px; border-radius: 10px;
  border: 1px solid #dee2e6; color: #212529; }
.bd-col-table { flex: 2 1 420px; }
.bd-col-chart { flex: 1 1 300px; text-align: center; }
.bd-col-h { font-size: 1.05em; font-weight: 700; color: #212529;
  margin-bottom: 12px; text-transform: uppercase;
  border-bottom: 2px solid #0d6efd; padding-bottom: 6px; text-align: left; }
/* table-layout: fixed -> cột không nhảy khi nội dung review đổi độ dài */
.bd-table { width: 100%; table-layout: fixed; border-collapse: collapse;
  background: #fff; font-size: .93em; }
.bd-table th { background: #212529; color: #fff; padding: 10px;
  text-align: left; font-weight: 600; }
.bd-table td { padding: 10px; border-bottom: 1px solid #dee2e6;
  vertical-align: middle; text-align: left; color: #212529;
  overflow-wrap: anywhere; word-break: break-word; }
.bd-badge { padding: 5px 11px; border-radius: 15px; font-weight: 600;
  display: inline-block; white-space: nowrap; }
/* khóa chiều cao ảnh -> biểu đồ đổi kích thước cũng không đẩy layout */
.bd-chart-img { width: 100%; height: 300px; object-fit: contain;
  border-radius: 6px; display: block; }
</style>
"""


def build_dashboard(rows, consumed_count, started_at, duration_seconds, fallback):
    with producer_lock:
        pstats = dict(producer_stats)

    elapsed = time.monotonic() - started_at

    # Banner phản ánh trạng thái thật thay vì luôn báo OK
    if pstats["error"] and pstats["delivered"] == 0 and not fallback:
        banner_cls, banner_txt = "bd-err", f"LỖI: {html_lib.escape(pstats['error'][:200])}"
    elif fallback:
        banner_cls = "bd-warn"
        banner_txt = "CHẾ ĐỘ DỰ PHÒNG: không gửi được lên OCI, đang đọc từ hàng đợi cục bộ"
    else:
        banner_cls = "bd-ok"
        banner_txt = "TRẠNG THÁI: HỆ THỐNG STREAMING ĐANG HOẠT ĐỘNG"

    table_rows = ""
    for r in reversed(rows[-TABLE_ROWS:]):
        rating = r["amazon_rating"]
        stars = "⭐" * max(1, min(5, int(round(rating))))
        # escape bắt buộc: reviewText có thể chứa <, >, &, " -> vỡ bảng
        title = html_lib.escape(r["title"][:MAX_TITLE_CHARS])
        if len(r["title"]) > MAX_TITLE_CHARS:
            title += "…"
        table_rows += (
            "<tr>"
            f'<td style="font-weight:700;text-align:center;">{rating} / 5.0<br>'
            f'<span style="font-size:.85em;color:#f39c12;letter-spacing:1px;">{stars}</span></td>'
            f"<td>{title}</td>"
            f'<td><span class="bd-badge" style="background:{r["b_color"]};'
            f'color:{r["t_color"]};">{html_lib.escape(r["emotion"])}</span></td>'
            "</tr>"
        )
    if not table_rows:
        table_rows = (
            '<tr><td colspan="3" style="text-align:center;padding:20px;">'
            "Đang chờ dữ liệu…</td></tr>"
        )

    chart_src = build_3d_chart(count_emotions(rows))

    return (
        DASHBOARD_CSS
        + '<div class="bd-wrap">'
        + '<h2 class="bd-title">Ứng dụng Big Data Streaming phân tích độ hài lòng khách hàng</h2>'
        + f'<div class="bd-banner {banner_cls}">{banner_txt}</div>'
        + '<div class="bd-stats">'
        + f'<div class="bd-card"><div class="bd-card-t">Đã xử lý</div>'
          f'<div class="bd-card-v">{pstats["generated"]:,}</div></div>'
        + f'<div class="bd-card"><div class="bd-card-t">Đã chuyển OCI</div>'
          f'<div class="bd-card-v bd-blue">{pstats["delivered"]:,}</div></div>'
        + f'<div class="bd-card"><div class="bd-card-t">Đã nhận</div>'
          f'<div class="bd-card-v bd-green">{consumed_count:,}</div></div>'
        + f'<div class="bd-card"><div class="bd-card-t">Thời gian</div>'
          f'<div class="bd-card-v">{elapsed:.0f}s / {duration_seconds:.0f}s</div></div>'
        + "</div>"
        + '<div class="bd-grid">'
        + '<div class="bd-col bd-col-table">'
          f'<div class="bd-col-h">Bảng {TABLE_ROWS} đánh giá gần nhất</div>'
          '<table class="bd-table"><thead><tr>'
          '<th style="width:22%;text-align:center;">Rating</th>'
          '<th style="width:48%;">Nội dung phản hồi</th>'
          '<th style="width:30%;">Phân tích cảm xúc (AI)</th>'
          f"</tr></thead><tbody>{table_rows}</tbody></table></div>"
        + '<div class="bd-col bd-col-chart">'
          '<div class="bd-col-h">Biểu đồ 3D phân phối cảm xúc</div>'
          f'<img class="bd-chart-img" src="{chart_src}" alt="Biểu đồ 3D"/></div>'
        + "</div></div>"
    )


def render_dashboard(slot, html_content):
    """st.html render thẳng vào DOM, KHÔNG qua Markdown -> không bị code block."""
    with slot.container():
        if hasattr(st, "html"):
            st.html(html_content)
        else:  # Streamlit < 1.33
            components.html(html_content, height=680, scrolling=False)


# ------------------------------------------------------------------------------
# 7. VÒNG LẶP STREAMING
# ------------------------------------------------------------------------------
def run_stream_demo(duration_seconds=DURATION_SECONDS):
    global use_fallback, sentiment_analyzer
    use_fallback = False

    if not SASL_USERNAME or not OCI_AUTH_TOKEN:
        st.error("Thiếu OCI_SASL_USERNAME hoặc OCI_AUTH_TOKEN trong Streamlit Secrets.")
        return pd.DataFrame()

    if sentiment_analyzer is None:
        sentiment_analyzer = load_sentiment_analyzer()

    stop_event = threading.Event()
    producer = Producer(PRODUCER_CONF)
    consumer = Consumer(CONSUMER_CONF)

    rows = []
    consumed_count = 0

    try:
        consumer.subscribe([TOPIC])
    except Exception as exc:
        st.error(f"Không subscribe được topic {TOPIC}: {exc}")
        return pd.DataFrame()

    producer_thread = threading.Thread(
        target=file_streaming_producer_worker, args=(producer, stop_event), daemon=True
    )
    producer_thread.start()

    started_at = time.monotonic()
    last_render = 0.0
    dash_slot = st.empty()
    dash_slot.info("Hệ thống đang khởi tạo luồng dữ liệu…")

    try:
        while time.monotonic() - started_at < duration_seconds:
            if not use_fallback and time.monotonic() - started_at > 12:
                with producer_lock:
                    if producer_stats["delivered"] == 0:
                        use_fallback = True

            if not use_fallback:
                message = consumer.poll(0.1)
            else:
                try:
                    message = MockMessage(local_queue.get(timeout=0.05))
                except queue.Empty:
                    message = None

            if message is not None and not message.error():
                try:
                    event = json.loads(message.value().decode("utf-8"))
                    if event.get("run_id") == RUN_ID:
                        consumed_count += 1
                        rows.append(event)
                except Exception:
                    pass

            now = time.monotonic()
            if now - last_render >= RENDER_INTERVAL:
                render_dashboard(
                    dash_slot,
                    build_dashboard(
                        rows, consumed_count, started_at, duration_seconds, use_fallback
                    ),
                )
                last_render = now
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        producer_thread.join(timeout=2)
        try:
            consumer.close()
        except Exception:
            pass

    render_dashboard(
        dash_slot,
        build_dashboard(rows, consumed_count, started_at, duration_seconds, use_fallback),
    )
    st.success("Quá trình streaming đã hoàn tất.")
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------
# 8. GIAO DIỆN
# ------------------------------------------------------------------------------
st.title("Ứng dụng Big Data Streaming phân tích độ hài lòng khách hàng")
st.caption("Amazon Fashion Reviews + OCI Streaming + RoBERTa Sentiment Analysis")

with st.sidebar:
    st.header("Thiết lập")
    duration_seconds = st.number_input(
        "Thời gian chạy (giây)",
        min_value=10,
        max_value=1800,
        value=DURATION_SECONDS,
        step=10,
        help="Trên Streamlit Cloud, chạy quá ~10 phút dễ bị ngắt kết nối websocket.",
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
        st.download_button(
            "Tải kết quả CSV",
            data=results_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="streaming_sentiment_results.csv",
            mime="text/csv",
        )
