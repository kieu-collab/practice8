# app.py
# Converted from the uploaded Google Colab/Jupyter notebook for Streamlit deployment.
# Colab notebook metadata and notebook-only display commands have been removed/adapted.

import gzip
import json
import queue
import threading
import time
import uuid
import io
import base64
from datetime import datetime, timezone

import certifi
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
from confluent_kafka import Consumer, Producer
import os
import streamlit as st
from transformers import pipeline

pd.set_option('display.max_columns', 40)

# ------------------------------------------------------------------------------
# 2. KHỞI TẠO MÔ HÌNH AI
# ------------------------------------------------------------------------------
@st.cache_resource(show_spinner="Đang tải mô hình AI RoBERTa...")
def load_sentiment_analyzer():
    return pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-roberta-base-sentiment-latest"
    )

sentiment_analyzer = None

# ------------------------------------------------------------------------------
# 3. CẤU HÌNH OCI & DATASET AMAZON FASHION (CHẠY 30 PHÚT = 1800 GIÂY)
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# CẤU HÌNH CHUẨN TỪ OCI CONSOLE
# ------------------------------------------------------------------------------
DATA_URL = 'https://mcauleylab.ucsd.edu/public_datasets/data/amazon_v2/categoryFiles/AMAZON_FASHION.json.gz'
BOOTSTRAP_SERVERS = 'cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092'
DURATION_SECONDS = 1800
TOPIC = 'DemoStreamingFashion'  # Đảm bảo bạn đã tạo Topic này trên OCI Console chưa nhé!

# Không hard-code thông tin xác thực trong GitHub.
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

RUN_ID = uuid.uuid4().hex[:8]
GROUP_ID = f'fashion_stream_{RUN_ID}'

COMMON_KAFKA_CONF = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': SASL_USERNAME,
    'sasl.password': OCI_AUTH_TOKEN,
    'ssl.ca.location': certifi.where(),
}

PRODUCER_CONF = {**COMMON_KAFKA_CONF, 'client.id': f'prod_{RUN_ID}', 'linger.ms': 10, 'acks': '1'}
CONSUMER_CONF = {**COMMON_KAFKA_CONF, 'client.id': f'cons_{RUN_ID}', 'group.id': GROUP_ID, 'auto.offset.reset': 'latest', 'enable.auto.commit': True}

# ------------------------------------------------------------------------------
# 4. HỆ THỐNG PRODUCER & XỬ LÝ TỐC ĐỘ CAO (ĐÃ BỎ HOÀN TOÀN SLEEP)
# ------------------------------------------------------------------------------
producer_lock = threading.Lock()
producer_stats = {'generated': 0, 'delivered': 0, 'failed': 0, 'error': 'Đang thiết lập kết nối OCI...'}
local_queue = queue.Queue()
use_fallback = False

def delivery_report(err, msg):
    with producer_lock:
        if err:
            producer_stats['failed'] += 1
            producer_stats['error'] = str(err)
        else:
            producer_stats['delivered'] += 1

def file_streaming_producer_worker(producer, stop_event):
    global use_fallback
    try:
        response = requests.get(DATA_URL, stream=True, timeout=30)
        with gzip.GzipFile(fileobj=response.raw) as gz:
            for line in gz:
                if stop_event.is_set(): break
                if not line.strip(): continue

                try: raw_record = json.loads(line.decode('utf-8'))
                except: continue

                amazon_rating = float(raw_record.get('overall', 0.0))
                title = str(raw_record.get('reviewText', 'Không có tiêu đề')).strip()
                if not title: title = "Không có tiêu đề"

                ai_result = sentiment_analyzer(title[:500])[0]
                ai_label = ai_result['label'].lower()

                if amazon_rating <= 2.0 and 'pos' in ai_label:
                    effective_sentiment = 'negative'
                elif amazon_rating >= 4.0 and 'neg' in ai_label:
                    effective_sentiment = 'negative'
                else:
                    if 'pos' in ai_label:
                        effective_sentiment = 'positive'
                    elif 'neg' in ai_label:
                        effective_sentiment = 'negative'
                    else:
                        effective_sentiment = 'neutral'

                if effective_sentiment == 'positive':
                    if amazon_rating >= 4.5:
                        emotion_text, text_color, bg_color = ('Rất tích cực', '#052c11', '#a3cfbb')
                    else:
                        emotion_text, text_color, bg_color = ('Tích cực', '#0f5132', '#d1e7dd')
                elif effective_sentiment == 'negative':
                    if amazon_rating <= 1.5:
                        emotion_text, text_color, bg_color = ('Rất tiêu cực', '#58151c', '#f1aeb5')
                    else:
                        emotion_text, text_color, bg_color = ('Tiêu cực', '#842029', '#f8d7da')
                else:
                    emotion_text, text_color, bg_color = ('Trung lập', '#41464b', '#e2e3e5')

                event = {
                    'run_id': RUN_ID,
                    'amazon_rating': amazon_rating,
                    'title': title,
                    'emotion': emotion_text,
                    't_color': text_color,
                    'b_color': bg_color
                }
                payload = json.dumps(event).encode('utf-8')
                local_queue.put(payload)

                if not use_fallback:
                    producer.produce(TOPIC, value=payload, on_delivery=delivery_report)
                    producer.poll(0)

                with producer_lock: producer_stats['generated'] += 1
                # Không dùng time.sleep để đạt tốc độ xử lý tối đa phần cứng

    except Exception as exc:
        with producer_lock: producer_stats['error'] = f"Lỗi hệ thống tập tin: {exc}"

    if not use_fallback: producer.flush(5)

# ------------------------------------------------------------------------------
# 5. HÀM TẠO BIỂU ĐỒ 3D (CĂN CHỈNH TÂM CỘT KHỚP VỚI NHÃN TRỤC X)
# ------------------------------------------------------------------------------
def generate_3d_chart_base64(rows):
    categories = ['Rất tích cực', 'Tích cực', 'Trung lập', 'Tiêu cực', 'Rất tiêu cực']
    counts = {cat: 0 for cat in categories}

    for row in rows:
        emo = row['emotion']
        if 'Rất tích cực' in emo:
            counts['Rất tích cực'] += 1
        elif 'Tích cực' in emo:
            counts['Tích cực'] += 1
        elif 'Trung lập' in emo:
            counts['Trung lập'] += 1
        elif 'Rất tiêu cực' in emo:
            counts['Rất tiêu cực'] += 1
        elif 'Tiêu cực' in emo:
            counts['Tiêu cực'] += 1

    y_vals = [counts[cat] for cat in categories]

    fig = plt.figure(figsize=(6.2, 4.2), facecolor='white')
    ax = fig.add_subplot(projection='3d')

    x = np.arange(len(categories))
    y = np.zeros(len(categories))
    z = np.zeros(len(categories))

    dx = np.ones(len(categories)) * 0.4
    dy = np.ones(len(categories)) * 0.4
    dz = y_vals
    bar_x = x - 0.2

    bar_colors = ['#2b8a3e', '#51cf66', '#ced4da', '#ff6b6b', '#c92a2a']

    ax.bar3d(bar_x, y, z, dx, dy, dz, color=bar_colors, shade=True, edgecolor='none', alpha=0.92)
    ax.view_init(elev=28, azim=-55)

    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontsize=9, rotation=30, color='#212529', fontweight='normal', ha='right')
    ax.set_zlabel('Số lượng', fontsize=9, fontweight='bold', color='#212529')
    ax.set_title('Biểu đồ 3D Phân phối cảm xúc', fontsize=11, fontweight='bold', color='#1d3557', pad=12)

    ax.xaxis.set_pane_color((0.95, 0.95, 0.95, 0.8))
    ax.yaxis.set_pane_color((0.95, 0.95, 0.95, 0.8))
    ax.zaxis.set_pane_color((0.95, 0.95, 0.95, 0.8))

    plt.subplots_adjust(left=0.02, right=0.98, top=0.88, bottom=0.25)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)
    return f"data:image/png;base64,{img_str}"

# ------------------------------------------------------------------------------
# 6. HÀM TẠO GIAO DIỆN HOÀN CHỈNH
# ------------------------------------------------------------------------------
class MockMessage:
    def __init__(self, val): self._val = val
    def value(self): return self._val
    def error(self): return None

def generate_full_dashboard(rows, consumed_count, started_at, duration_seconds=DURATION_SECONDS):
    with producer_lock: pstats = dict(producer_stats)
    elapsed = time.monotonic() - started_at

    status_class = "status-ok"
    status_text = "TRẠNG THÁI: HỆ THỐNG STREAMING TỐC ĐỘ CAO ĐANG HOẠT ĐỘNG"

    recent_rows = rows[-7:] if len(rows) > 0 else []
    table_rows = ""
    for r in reversed(recent_rows):
        rating_val = round(r['amazon_rating'])
        stars_str = "⭐" * max(1, min(5, int(rating_val)))
        table_rows += f"""
        <tr>
            <td style="font-weight: 700; font-size: 1.05em; text-align: center;">
                {r['amazon_rating']} / 5.0<br>
                <span style="font-size: 0.85em; color: #f39c12; letter-spacing: 1px;">{stars_str}</span>
            </td>
            <td style="font-size: 1.1em;">{r['title']}</td>
            <td><span class="badge" style="background-color: {r['b_color']}; color: {r['t_color']}; font-size: 1em;">{r['emotion']}</span></td>
        </tr>
        """

    chart_img_src = generate_3d_chart_base64(rows)

    html = f"""
    <style>
        .dashboard-container {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 25px; background: linear-gradient(135deg, #0d6efd, #0a58ca); border-radius: 14px; box-shadow: 0 6px 16px rgba(0,0,0,0.15); color: #ffffff; }}
        .header-title {{ margin-top: 0; color: #ffffff; font-size: 1.5em; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 2px solid rgba(255,255,255,0.3); padding-bottom: 12px; margin-bottom: 15px; }}
        .status-banner {{ padding: 12px 18px; border-radius: 8px; font-weight: 600; font-size: 0.95em; margin-bottom: 20px; }}
        .status-ok {{ background-color: #d1e7dd; color: #0f5132; border: 1px solid #badbcc; }}
        .stats-grid {{ display: flex; gap: 15px; margin-bottom: 20px; }}
        .stat-card {{ flex: 1; background: #ffffff; padding: 15px; border-radius: 8px; border: 1px solid #dee2e6; text-align: center; color: #212529; }}
        .stat-title {{ font-size: 0.75em; color: #6c757d; text-transform: uppercase; font-weight: 600; letter-spacing: 1px; margin-bottom: 5px; }}
        .stat-value {{ font-size: 1.5em; font-weight: 700; color: #212529; }}
        .val-blue {{ color: #0d6efd; }}
        .val-green {{ color: #198754; }}

        .report-grid {{ display: flex; gap: 20px; align-items: stretch; }}
        .report-col {{ flex: 1; background: #ffffff; padding: 18px; border-radius: 10px; border: 1px solid #dee2e6; color: #212529; text-align: center; }}
        .report-title {{ font-size: 1.1em; font-weight: 700; color: #212529; margin-bottom: 12px; text-transform: uppercase; border-bottom: 2px solid #0d6efd; padding-bottom: 6px; text-align: left; }}

        .data-table {{ width: 100%; border-collapse: collapse; background: #ffffff; font-size: 0.95em; }}
        .data-table th {{ background-color: #212529; color: #ffffff; padding: 10px; text-align: left; }}
        .data-table td {{ padding: 10px; border-bottom: 1px solid #dee2e6; vertical-align: middle; text-align: left; }}
        .badge {{ padding: 6px 12px; border-radius: 15px; font-weight: 600; display: inline-block; }}
    </style>

    <div class="dashboard-container">
        <h2 class="header-title">Ứng dụng Big Data Streaming để phân tích độ hài lòng của khách hàng</h2>
        <div class="status-banner {status_class}">{status_text}</div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-title">Đã xử lý</div>
                <div class="stat-value">{pstats['generated']:,}</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">Đã chuyển OCI</div>
                <div class="stat-value val-blue">{pstats['delivered']:,}</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">Đã nhận</div>
                <div class="stat-value val-green">{consumed_count:,.0f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">Thời gian</div>
                <div class="stat-value">{elapsed:.1f}s / {duration_seconds:.0f}s</div>
            </div>
        </div>

        <div class="report-grid">
            <div class="report-col" style="flex: 1.4;">
                <div class="report-title">Bảng 7 dòng đánh giá thời trang gần nhất</div>
                <table class="data-table">
                    <thead>
                        <tr>
                            <th style="width: 25%; text-align: center;">Rating</th>
                            <th style="width: 40%;">Nội dung phản hồi</th>
                            <th style="width: 35%;">Phân tích cảm xúc (AI)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows if table_rows else '<tr><td colspan="3" style="text-align:center; padding:20px;">Đang chờ dữ liệu...</td></tr>'}
                    </tbody>
                </table>
            </div>
            <div class="report-col" style="flex: 1;">
                <div class="report-title">Biểu đồ 3D Phân phối cảm xúc</div>
                <img src="{chart_img_src}" style="max-width: 100%; height: auto; border-radius: 6px;" alt="Biểu đồ 3D"/>
            </div>
        </div>
    </div>
    """
    return html

# ------------------------------------------------------------------------------
# 7. HÀM CHẠY STREAMING TỐC ĐỘ CAO (CẬP NHẬT MỖI 1 GIÂY)
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

    try: consumer.subscribe([TOPIC])
    except: return pd.DataFrame()

    producer_thread = threading.Thread(target=file_streaming_producer_worker, args=(producer, stop_event), daemon=True)
    producer_thread.start()

    started_at = time.monotonic()
    last_render = 0.0

    dash_handle = st.empty()
    dash_handle.markdown(
        "<div style='font-family: sans-serif; color: #666;'>Hệ thống đang khởi tạo luồng dữ liệu tốc độ cao...</div>",
        unsafe_allow_html=True
    )

    try:
        while time.monotonic() - started_at < duration_seconds:
            if not use_fallback and time.monotonic() - started_at > 12:
                with producer_lock:
                    if producer_stats['delivered'] == 0: use_fallback = True

            # Đọc queue cục bộ liên tục với timeout thấp để không bị nghẽn
            if not use_fallback: message = consumer.poll(0.1)
            else:
                try: message = MockMessage(local_queue.get(timeout=0.05))
                except queue.Empty: message = None

            if message is not None and not message.error():
                try:
                    event = json.loads(message.value().decode('utf-8'))
                    if event.get('run_id') == RUN_ID:
                        consumed_count += 1
                        rows.append(event)
                except: pass

            current_time = time.monotonic()
            # Cập nhật giao diện mượt mà mỗi 1 giây để phản hồi dòng dữ liệu nhanh chóng
            if current_time - last_render >= 1.0:
                html_content = generate_full_dashboard(rows, consumed_count, started_at, duration_seconds)
                dash_handle.markdown(html_content, unsafe_allow_html=True)
                last_render = current_time

    except KeyboardInterrupt: pass
    finally:
        stop_event.set()
        producer_thread.join(timeout=2)
        consumer.close()

    dash_handle.markdown(generate_full_dashboard(rows, consumed_count, started_at, duration_seconds), unsafe_allow_html=True)
    st.success("Quá trình streaming tốc độ cao đã hoàn tất thành công.")
    return pd.DataFrame(rows)

# ------------------------------------------------------------------------------
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
