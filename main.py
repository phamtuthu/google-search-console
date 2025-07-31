import os
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric
)
from google.oauth2 import service_account
from clickhouse_driver import Client

print("========== Script started ==========")

# --- Tạo credentials từ biến môi trường ---
CREDENTIALS_ENV = os.environ.get("GA4_CREDENTIALS_JSON")
CREDENTIALS_PATH = "ga4-credentials.json"
if CREDENTIALS_ENV and not os.path.exists(CREDENTIALS_PATH):
    with open(CREDENTIALS_PATH, "w") as f:
        f.write(CREDENTIALS_ENV)
    print("✅ Created ga4-credentials.json from env variable.")

# --- Cấu hình cố định ---
START_DATE = "2025-01-01"
END_DATE = "2025-01-31"
MAX_ROWS = int(os.environ.get("MAX_ROWS", 30000))

CH_HOST = os.environ.get("CH_HOST")
CH_PORT = int(os.environ.get("CH_PORT", 9000))
CH_USER = os.environ.get("CH_USER")
CH_PASSWORD = os.environ.get("CH_PASSWORD")
CH_DATABASE = os.environ.get("CH_DATABASE")
CH_TABLE = os.environ.get("CH_TABLE")
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID")
GOOGLE_CREDENTIALS_FILE = "ga4-credentials.json"

# --- Kết nối GA4 ---
credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=["https://www.googleapis.com/auth/analytics.readonly"],
)
ga4_client = BetaAnalyticsDataClient(credentials=credentials)

# --- Kết nối ClickHouse ---
ch_client = Client(
    host=CH_HOST,
    port=CH_PORT,
    user=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE
)

# --- Khai báo dimensions & metrics ---
dimensions = [
    Dimension(name="date"),
    Dimension(name="landingPagePlusQueryString"),
]

metrics = [
    Metric(name="organicGoogleSearchClicks"),
    Metric(name="organicGoogleSearchImpressions"),
    Metric(name="organicGoogleSearchAveragePosition"),
]

# --- Phân trang lấy dữ liệu ---
offset = 0
page = 1
all_rows = []

while True:
    print(f"🔄 Fetching GA4 page {page}, offset {offset}")
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=dimensions,
        metrics=metrics,
        date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
        limit=MAX_ROWS,
        offset=offset
    )

    response = ga4_client.run_report(request)
    rows = []

    for row in response.rows:
        dim_vals = [f.value for f in row.dimension_values]
        met_vals = [f.value for f in row.metric_values]

        # Parse date
        date_str = dim_vals[0]
        date_vn = datetime.strptime(date_str, "%Y%m%d") + timedelta(hours=7)

        landing_page = dim_vals[1]
        clicks = int(float(met_vals[0] or 0))
        impressions = int(float(met_vals[1] or 0))
        avg_position = float(met_vals[2] or 0.0)

        ctr = round(clicks / impressions, 4) if impressions > 0 else 0.0

        rows.append([
            date_vn, landing_page, clicks, impressions, ctr, avg_position
        ])

    print(f"✅ Fetched {len(rows)} rows.")
    all_rows.extend(rows)

    if len(rows) < MAX_ROWS:
        break
    offset += MAX_ROWS
    page += 1

print(f"📦 Tổng cộng: {len(all_rows)} rows")

# --- Insert vào ClickHouse ---
if all_rows:
    try:
        insert_query = f"""
            INSERT INTO {CH_TABLE} (
                date, landing_page, clicks, impressions, ctr, avg_position
            ) VALUES
        """
        ch_client.execute(insert_query, all_rows)
        print(f"🚀 Đã insert {len(all_rows)} rows vào ClickHouse table {CH_TABLE}")
    except Exception as e:
        print("❌ Lỗi insert vào ClickHouse:", str(e))
else:
    print("⚠️ Không có dữ liệu để insert!")

print("========== Script finished ==========")
