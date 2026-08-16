'''
from pathlib import Path

import pandas as pd
import plotly.express as px
import json
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from src.air_quality import get_spark, load_data, load_model, prepare_spark_data  # noqa: E402

st.set_page_config(page_title="Giám sát chất lượng không khí", page_icon=":material/air:", layout="wide")

@st.cache_data(ttl=15, max_entries=1)
def get_live_data():
    data = load_data().copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
    return data.dropna(subset=["timestamp"]).sort_values("timestamp")

@st.cache_resource
def get_resources():
    spark = get_spark()
    model, _ = load_model(spark)
    return spark, model

def aqi_label(value):
    return "Tốt" if value <= 50 else "Trung bình" if value <= 100 else "Kém" if value <= 150 else "Xấu" if value <= 200 else "Rất xấu"

st.title("Giám sát chất lượng không khí")
st.caption("Theo dõi dữ liệu theo giờ và dự báo PM2.5 sau 1 giờ bằng Apache Spark MLlib")
data = get_live_data()
locations = data[["location_id", "location_name"]].drop_duplicates().sort_values("location_name")
with st.sidebar:
    st.header("Bộ lọc")
    selected_name = st.selectbox(
        "Địa điểm", locations["location_name"].tolist(),
        index=locations["location_name"].tolist().index(st.session_state.selected_location_name),
        key="location_selector", on_change=sync_location_selection,
    )
    days = st.slider("Khoảng thời gian hiển thị (ngày)", 1, 30, 7)
    refresh_seconds = st.select_slider("Chu kỳ cập nhật", options=[10, 15, 30, 60], value=15, format_func=lambda x: f"{x} giây")
    if st.button(":material/refresh: Làm mới ngay", width="stretch"):
        st.cache_data.clear()
        st.rerun()
selected_id = locations.loc[locations["location_name"] == selected_name, "location_id"].iloc[0]

@st.fragment(run_every=f"{refresh_seconds}s")
def live_dashboard():
    live = get_live_data()
    view = live[live["location_id"] == selected_id].tail(days * 24).copy()
    if view.empty:
        st.error("Không có dữ liệu cho địa điểm đã chọn.")
        return
    spark, model = get_resources()
    prepared = prepare_spark_data(spark)
    current = prepared.filter(prepared.location_id == selected_id).orderBy(prepared.timestamp.desc()).limit(1)
    prediction = float(model.transform(current).select("prediction").first()[0])
    latest = view.iloc[-1]
    previous = view.iloc[-2] if len(view) > 1 else latest
    st.success(f":material/circle: Tự cập nhật mỗi {refresh_seconds} giây · Bản ghi mới nhất: {latest['timestamp']:%d/%m/%Y %H:%M}")
    with st.container(horizontal=True):
        st.metric("PM2.5 hiện tại", f"{latest['pm25']:.1f} µg/m³", f"{latest['pm25'] - previous['pm25']:+.1f}", border=True)
        st.metric("Dự báo sau 1 giờ", f"{prediction:.1f} µg/m³", border=True)
        st.metric("AQI hiện tại", f"{latest['aqi']:.0f}", aqi_label(latest["aqi"]), border=True)
        st.metric("Trạm quan trắc", selected_name, border=True)
    chart = px.line(view, x="timestamp", y="pm25", markers=True, title=f"Diễn biến PM2.5 · {selected_name}")
    chart.update_layout(xaxis_title="Thời gian", yaxis_title="PM2.5 (µg/m³)", margin=dict(l=20, r=20, t=55, b=20))
    st.plotly_chart(chart, width="stretch")
    with st.container(border=True):
        st.subheader("Bản ghi gần nhất")
        recent = view.tail(10).sort_values("timestamp", ascending=False)
        st.dataframe(recent[["timestamp", "pm25", "pm10", "aqi", "co", "no2", "so2", "o3"]], hide_index=True, width="stretch")

live_dashboard()
st.info("Nguồn dữ liệu: dataset nhiều địa điểm trong data/raw. Có thể thay get_live_data() bằng API hoặc Kafka consumer khi triển khai production.")
"""

from pathlib import Path

import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from src.air_quality import get_spark, load_data, load_model, prepare_spark_data  # noqa: E402

st.set_page_config(page_title="Air Quality Forecast", page_icon="AQ", layout="wide")
st.title("Dự báo chất lượng không khí")
st.caption("Apache Spark MLlib | Dự báo PM2.5 sau 1 giờ")

@st.cache_resource
def resources():
    spark = get_spark()
    model, _ = load_model(spark)
    return spark, model

raw = load_data()
locations = raw[["location_id", "location_name"]].drop_duplicates().sort_values("location_name")
selected_name = st.sidebar.selectbox("Chọn địa điểm", locations["location_name"].tolist())
selected_id = locations.loc[locations["location_name"] == selected_name, "location_id"].iloc[0]
days = st.sidebar.slider("Số ngày hiển thị", 1, 30, 7)

spark, model = resources()
prepared = prepare_spark_data(spark)
view_spark = prepared.filter(prepared.location_id == selected_id).orderBy("timestamp")
view = view_spark.toPandas().tail(days * 24)
if view.empty:
    st.error("Không có đủ dữ liệu cho địa điểm đã chọn.")
    st.stop()

prediction = model.transform(view_spark.orderBy(view_spark.timestamp.desc()).limit(1)).select("prediction").first()[0]
latest = view.iloc[-1]
c1, c2, c3 = st.columns(3)
c1.metric("PM2.5 hiện tại", f"{latest['pm25']:.2f} µg/m³")
c2.metric("Dự báo sau 1 giờ", f"{prediction:.2f} µg/m³")
c3.metric("AQI hiện tại", f"{latest['aqi']:.0f}")

st.plotly_chart(px.line(view, x="timestamp", y="pm25", title=f"PM2.5 tại {selected_name}"), use_container_width=True)
metrics = model.stages[-1].getOrDefault("featuresCol") if False else None
st.info("Mô hình được huấn luyện bằng Apache Spark MLlib trên dữ liệu của 4 địa điểm.")
"""
'''
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import json
import plotly.graph_objects as go

STREAM_PATH = Path(__file__).resolve().parent / "data" / "streaming" / "air_quality"
PREDICTION_PATH = Path(__file__).resolve().parent / "data" / "streaming" / "multi_horizon_predictions"
GEOJSON_PATH = Path(__file__).resolve().parent / "data" / "geo" / "vietnam_adm1.geojson"

st.set_page_config(
    page_title="Giám sát chất lượng không khí realtime",
    page_icon=":material/air:",
    layout="wide",
)


@st.cache_data(ttl=10, max_entries=1)
def load_stream_data(path: str) -> pd.DataFrame:
    data = pd.read_parquet(path)
    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce", utc=True)
    if "forecast_for" in data.columns:
        data["forecast_for"] = pd.to_datetime(data["forecast_for"], errors="coerce", utc=True)
    return data.dropna(subset=["timestamp"]).sort_values("timestamp")


@st.cache_data
def load_geojson(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


@st.cache_data(ttl=10, max_entries=1)
def load_predictions(path: str) -> pd.DataFrame:
    if not Path(path).exists() or not any(Path(path).glob("*.parquet")):
        return pd.DataFrame()
    data = pd.read_parquet(path)
    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce", utc=True)
    if "forecast_for" in data.columns:
        data["forecast_for"] = pd.to_datetime(data["forecast_for"], errors="coerce", utc=True)
    return data.dropna(subset=["timestamp"]).sort_values("timestamp")


def aqi_status(value: float) -> str:
    if value <= 50:
        return "Tốt"
    if value <= 100:
        return "Trung bình"
    if value <= 150:
        return "Kém"
    if value <= 200:
        return "Xấu"
    return "Rất xấu"


st.title("Giám sát chất lượng không khí realtime")
st.caption("Open-Meteo API → Kafka → Spark Structured Streaming → Parquet → Streamlit")

if not STREAM_PATH.exists() or not any(STREAM_PATH.glob("*.parquet")):
    st.warning("Chưa có dữ liệu streaming. Hãy chạy Kafka, Spark Streaming và live_producer.py trước.")
    st.code(".\\.venv\\Scripts\\python.exe streaming\\live_producer.py", language="powershell")
    st.stop()

def aqi_guidance(value: float) -> dict:
    if value <= 20:
        return {"label": "Không khí trong lành", "color": "green", "advice": "Bạn có thể sinh hoạt và vận động ngoài trời bình thường."}
    if value <= 40:
        return {"label": "Chất lượng khá", "color": "green", "advice": "Nhìn chung an toàn cho mọi người."}
    if value <= 60:
        return {"label": "Cần lưu ý", "color": "yellow", "advice": "Người nhạy cảm nên giảm vận động mạnh ngoài trời."}
    if value <= 80:
        return {"label": "Không khí kém", "color": "orange", "advice": "Nên hạn chế hoạt động ngoài trời kéo dài."}
    if value <= 100:
        return {"label": "Không khí rất kém", "color": "red", "advice": "Trẻ em, người cao tuổi và người bệnh hô hấp nên ở trong nhà."}
    return {"label": "Ô nhiễm nghiêm trọng", "color": "red", "advice": "Hạn chế ra ngoài và đóng cửa sổ nếu có thể."}


def aqi_status(value: float) -> str:
    return aqi_guidance(value)["label"]


def pm25_guidance(value: float) -> dict:
    if value <= 10:
        return {"label": "Thấp", "color": "green", "advice": "Mức bụi mịn dự kiến thấp, phù hợp cho hoạt động ngoài trời."}
    if value <= 20:
        return {"label": "Khá", "color": "green", "advice": "Chất lượng không khí dự kiến nhìn chung chấp nhận được."}
    if value <= 25:
        return {"label": "Cần lưu ý", "color": "yellow", "advice": "Người nhạy cảm nên cân nhắc giảm vận động mạnh ngoài trời."}
    if value <= 50:
        return {"label": "Cao", "color": "orange", "advice": "Nên hạn chế hoạt động ngoài trời kéo dài."}
    if value <= 75:
        return {"label": "Rất cao", "color": "red", "advice": "Nhóm nhạy cảm nên ở trong nhà và giảm tiếp xúc không khí ngoài trời."}
    return {"label": "Nghiêm trọng", "color": "red", "advice": "Hạn chế ra ngoài; dự báo này cần được theo dõi sát."}


data = load_stream_data(str(STREAM_PATH))
locations = data[["location_id", "location_name"]].drop_duplicates().sort_values("location_name")

if "selected_location_name" not in st.session_state:
    st.session_state.selected_location_name = locations["location_name"].iloc[0]
if "show_location_details" not in st.session_state:
    st.session_state.show_location_details = False
if "map_view_id" not in st.session_state:
    st.session_state.map_view_id = 0
if "pending_location_name" in st.session_state:
    st.session_state.selected_location_name = st.session_state.pop("pending_location_name")


def sync_location_selection():
    st.session_state.selected_location_name = st.session_state.location_selector

selected_name = st.session_state.selected_location_name
forecast_hours = 1
refresh_seconds = 15
if st.session_state.show_location_details:
    with st.sidebar:
        st.header("Bộ lọc dự báo")
        selected_name = st.selectbox(
            "Địa điểm", locations["location_name"].tolist(),
            index=locations["location_name"].tolist().index(st.session_state.selected_location_name),
            key="location_selector", on_change=sync_location_selection,
        )
        forecast_hours = st.slider("Dự báo sau (giờ)", 1, 24, 1)
        refresh_seconds = st.select_slider(
            "Chu kỳ cập nhật", options=[10, 15, 30, 60], value=15,
            format_func=lambda seconds: f"{seconds} giây",
        )
        if st.button(":material/refresh: Làm mới ngay", width="stretch"):
            st.cache_data.clear()
            st.rerun()

selected_id = locations.loc[locations["location_name"] == selected_name, "location_id"].iloc[0]


def render_location_details(live: pd.DataFrame):
    if st.button(":material/arrow_back: Quay lại bản đồ", key="detail_back_to_map"):
        st.session_state.show_location_details = False
        st.session_state.map_view_id += 1
        st.rerun()

    view = live[live["location_id"] == selected_id].copy()
    if view.empty:
        st.warning("Chưa nhận được dữ liệu cho địa điểm này.")
        return

    latest = view.iloc[-1]
    previous = view.iloc[-2] if len(view) > 1 else latest
    quality = aqi_guidance(float(latest["aqi"]))
    predictions = load_predictions(str(PREDICTION_PATH))
    prediction_view = predictions[
        (predictions["location_id"] == selected_id)
        & (predictions["timestamp"] == latest["timestamp"])
        & (predictions["horizon_hours"] == forecast_hours)
    ] if not predictions.empty else pd.DataFrame()
    prediction_row = prediction_view.iloc[-1] if not prediction_view.empty else None
    predicted_pm25 = prediction_row["predicted_pm25"] if prediction_row is not None else None
    forecast_time = prediction_row["forecast_for"].tz_convert("Asia/Bangkok") if prediction_row is not None else None
    forecast_quality = pm25_guidance(float(predicted_pm25)) if predicted_pm25 is not None else None
    source_time = latest["timestamp"].tz_convert("Asia/Bangkok")

    st.header(f"Chất lượng không khí tại {selected_name}")
    st.caption(f"Bản ghi mới nhất: {source_time:%d/%m/%Y %H:%M} ICT")
    with st.container(border=True):
        st.badge(quality["label"], color=quality["color"])
        st.write(quality["advice"])

    with st.container(horizontal=True):
        st.metric("PM2.5 hiện tại", f"{latest['pm25']:.1f} µg/m³", f"{latest['pm25'] - previous['pm25']:+.1f}", border=True)
        st.metric("PM10 hiện tại", f"{latest['pm10']:.1f} µg/m³", border=True)
        st.metric(f"Dự báo sau {forecast_hours} giờ", f"{predicted_pm25:.1f} µg/m³" if predicted_pm25 is not None else "Đang chờ dữ liệu", border=True)
        st.metric("European AQI", f"{latest['aqi']:.0f}", quality["label"], border=True)

    if forecast_quality is not None:
        with st.container(border=True):
            st.badge(f"PM2.5 dự kiến: {forecast_quality['label']}", color=forecast_quality["color"])
            st.write(forecast_quality["advice"])
            st.caption(f"Thời điểm dự báo: {forecast_time:%d/%m/%Y %H:%M} ICT · Đây là giá trị do model ước tính, không phải số đo thực tế.")

    with st.expander("Ý nghĩa các chỉ số", icon=":material/info:"):
        st.markdown("""
| Chỉ số | Ý nghĩa |
|---|---|
| **PM2.5** | Bụi mịn có đường kính không quá 2,5 µm; có thể đi sâu vào phổi. |
| **PM10** | Hạt bụi có đường kính không quá 10 µm; có thể ảnh hưởng đường hô hấp. |
| **European AQI** | Chỉ số tổng hợp mô tả chất lượng không khí hiện tại; số càng cao thì ô nhiễm càng lớn. |
| **Dự báo PM2.5** | Giá trị PM2.5 được model ước tính cho thời điểm tương lai đã chọn. |

Đơn vị `µg/m³` nghĩa là microgam chất ô nhiễm trong một mét khối không khí.

> Dự báo mang tính tham khảo và có thể có sai số. Người có bệnh hô hấp nên ưu tiên hướng dẫn từ cơ quan y tế và môi trường chính thức.
""")

    chart = px.line(view, x="timestamp", y=["pm25", "pm10"], markers=True, title=f"Diễn biến PM2.5 và PM10 tại {selected_name}")
    chart.update_layout(xaxis_title="Thời gian", yaxis_title="Nồng độ (µg/m³)", legend_title_text="Chỉ số", margin=dict(l=20, r=20, t=55, b=20))
    st.plotly_chart(chart, width="stretch")

    with st.container(border=True):
        st.subheader("Bản ghi gần nhất")
        recent = view.tail(20).sort_values("timestamp", ascending=False)
        st.dataframe(recent[["timestamp", "pm25", "pm10", "aqi", "co", "no2", "so2", "o3"]], hide_index=True, width="stretch")


@st.fragment(run_every=f"{refresh_seconds}s")
def render_live_dashboard():
    live = load_stream_data(str(STREAM_PATH))
    if st.session_state.show_location_details:
        render_location_details(live)
        return

    stations = live.sort_values("timestamp").groupby("location_id", as_index=False).tail(1).copy()
    stations["quality"] = stations["aqi"].map(lambda value: aqi_guidance(float(value))["label"])
    st.caption("Đang theo dõi 4 địa điểm có dữ liệu realtime: TP.HCM, Hà Nội, Đà Nẵng và Cần Thơ. Nhấn vào một điểm để xem chi tiết.")
    geojson = load_geojson(str(GEOJSON_PATH))
    city_iso = {"hanoi": "VN-HN", "danang": "VN-DN", "hcm": "VN-SG", "cantho": "VN-CT"}
    stations["shape_iso"] = stations["location_id"].map(city_iso)
    all_provinces = [feature["properties"]["shapeISO"] for feature in geojson["features"]]
    map_figure = go.Figure()
    map_figure.add_trace(go.Choroplethmap(
        geojson=geojson, locations=all_provinces, z=[0] * len(all_provinces),
        featureidkey="properties.shapeISO", colorscale=[[0, "#e5e7eb"], [1, "#e5e7eb"]],
        showscale=False, hoverinfo="skip", marker_line_color="#94a3b8", marker_line_width=0.7,
    ))
    map_figure.add_trace(go.Choroplethmap(
        geojson=geojson, locations=stations["shape_iso"], z=stations["aqi"],
        featureidkey="properties.shapeISO", colorscale="RdYlGn_r", zmin=0,
        zmax=max(100, float(stations["aqi"].max())), colorbar_title="European AQI",
        customdata=stations[["location_name", "pm25", "pm10", "aqi", "quality"]].to_numpy(),
        hovertemplate="<b>%{customdata[0]}</b><br>PM2.5: %{customdata[1]:.1f} µg/m³<br>PM10: %{customdata[2]:.1f} µg/m³<br>AQI: %{customdata[3]:.0f}<br>%{customdata[4]}<extra></extra>",
        marker_line_color="#0f172a", marker_line_width=1.5,
    ))
    map_figure.add_trace(go.Scattermap(
        lat=stations["latitude"], lon=stations["longitude"], mode="text",
        text=stations["location_name"], textfont={"size": 12, "color": "#111827"},
        customdata=stations[["location_name"]].to_numpy(), hoverinfo="skip", showlegend=False,
    ))
    map_figure.update_layout(
        title="Bản đồ chất lượng không khí tại Việt Nam",
        map={"style": "white-bg", "center": {"lat": 16.0, "lon": 106.0}, "zoom": 4.6, "bounds": {"west": 102.0, "east": 110.0, "south": 8.0, "north": 24.0}},
        height=720, margin=dict(l=0, r=0, t=45, b=0), showlegend=False,
    )
    selection = st.plotly_chart(map_figure, width="stretch", key=f"air_quality_map_{st.session_state.map_view_id}", on_select="rerun", selection_mode="points")
    points = selection.get("selection", {}).get("points", [])
    if points:
        point = points[0]
        iso_to_name = {"VN-HN": "Hà Nội", "VN-DN": "Đà Nẵng", "VN-SG": "TP.HCM", "VN-CT": "Cần Thơ"}
        clicked_name = iso_to_name.get(point.get("location"))
        if clicked_name is None and point.get("customdata"):
            clicked_name = point["customdata"][0]
        if clicked_name in locations["location_name"].tolist():
            st.session_state.pending_location_name = clicked_name
            st.session_state.show_location_details = True
            st.rerun()

    if not st.session_state.show_location_details:
        return

    if st.button(":material/arrow_back: Quay lại bản đồ", key="back_to_map"):
        st.session_state.show_location_details = False
        st.session_state.map_view_id += 1
        st.rerun()

    view = live[live["location_id"] == selected_id].copy()
    if view.empty:
        st.warning("Chưa nhận được bản ghi streaming cho địa điểm này.")
        return

    latest = view.iloc[-1]
    quality = aqi_guidance(float(latest["aqi"]))
    previous = view.iloc[-2] if len(view) > 1 else latest
    predictions = load_predictions(str(PREDICTION_PATH))
    prediction_view = predictions[(predictions["location_id"] == selected_id) & (predictions["timestamp"] == latest["timestamp"]) & (predictions["horizon_hours"] == forecast_hours)] if not predictions.empty else pd.DataFrame()
    predicted_pm25 = prediction_view.iloc[-1]["predicted_pm25"] if not prediction_view.empty else None
    source_time = latest["timestamp"].tz_convert("Asia/Bangkok")
    st.success(f":material/circle: Đang nhận dữ liệu realtime · Bản ghi mới nhất: {source_time:%d/%m/%Y %H:%M} ICT")

    with st.container(border=True):
        st.badge(quality["label"], color=quality["color"])
        st.write(quality["advice"])
        st.caption("Phân loại theo European AQI của Open-Meteo. AQI, PM2.5 và PM10 bên dưới là các giá trị chi tiết.")

    with st.container(horizontal=True):
        st.metric("PM2.5 hiện tại", f"{latest['pm25']:.1f} µg/m³", f"{latest['pm25'] - previous['pm25']:+.1f}", border=True)
        st.metric("PM10 hiện tại", f"{latest['pm10']:.1f} µg/m³", border=True)
        st.metric(f"Dự báo PM2.5 sau {forecast_hours} giờ", f"{predicted_pm25:.1f} µg/m³" if predicted_pm25 is not None else "Đang chờ dữ liệu", border=True)
        st.metric("AQI hiện tại", f"{latest['aqi']:.0f}", aqi_status(latest["aqi"]), border=True)
        st.metric("Trạm quan trắc", selected_name, border=True)

    chart = px.line(view, x="timestamp", y=["pm25", "pm10"], markers=True, title=f"PM2.5 và PM10 · {selected_name}")
    chart.update_layout(xaxis_title="Thời gian", yaxis_title="Nồng độ (µg/m³)", legend_title_text="Chỉ số", margin=dict(l=20, r=20, t=55, b=20))
    st.plotly_chart(chart, width="stretch")

    with st.container(border=True):
        st.subheader("Bản ghi nhận được gần nhất")
        recent = view.tail(20).sort_values("timestamp", ascending=False)
        st.dataframe(
            recent[["timestamp", "pm25", "pm10", "aqi", "co", "no2", "so2", "o3"]],
            hide_index=True,
            width="stretch",
        )


render_live_dashboard()
if st.session_state.show_location_details:
    st.caption("Dữ liệu được cập nhật từ Kafka và Spark Structured Streaming.")
