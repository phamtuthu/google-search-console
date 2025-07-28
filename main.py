import os
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
)
from google.oauth2 import service_account
from clickhouse_driver import Client

print("========== Script started ==========")
print("Python version:", __import__('sys').version)

# --- Check and create credentials file from env if needed ---
CREDENTIALS_ENV = os.environ.get("GA4_CREDENTIALS_JSON")
CREDENTIALS_PATH = "ga4-credentials.json"

if CREDENTIALS_ENV and not os.path.exists(CREDENTIALS_PATH):
    with open(CREDENTIALS_PATH, "w") as f:
        f.write(CREDENTIALS_ENV)
    print("Created ga4-credentials.json from env variable.")
elif os.path.exists(CREDENTIALS_PATH):
    print("ga4-credentials.json file exists.")
else:
    print("No credentials file found! Script will fail if continue.")

GOOGLE_CREDENTIALS_FILE = CREDENTIALS_PATH

# --- Config from ENV ---
START_DATE = os.environ.get("START_DATE", "2025-07-01")
END_DATE = os.environ.get("END_DATE", "2025-07-01")
MAX_ROWS = int(os.environ.get("MAX_ROWS", 30000))
CH_HOST = os.environ.get("CH_HOST")
CH_PORT = int(os.environ.get("CH_PORT", 9000))
CH_USER = os.environ.get("CH_USER")
CH_PASSWORD = os.environ.get("CH_PASSWORD")
CH_DATABASE = os.environ.get("CH_DATABASE")
CH_TABLE = os.environ.get("CH_TABLE")
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID")
print("CH_HOST:", CH_HOST)
print("CH_PORT:", CH_PORT)
print("CH_USER:", CH_USER)
print("CH_DATABASE:", CH_DATABASE)
print("CH_TABLE:", CH_TABLE)


# --- Print all config to log ---
print(f"START_DATE: {START_DATE}")
print(f"END_DATE: {END_DATE}")
print(f"MAX_ROWS: {MAX_ROWS}")
print(f"CH_HOST: {CH_HOST}")
print(f"CH_PORT: {CH_PORT}")
print(f"CH_USER: {CH_USER}")
print(f"CH_DATABASE: {CH_DATABASE}")
print(f"CH_TABLE: {CH_TABLE}")
print(f"GA4_PROPERTY_ID: {GA4_PROPERTY_ID}")
print(f"GOOGLE_CREDENTIALS_FILE: {GOOGLE_CREDENTIALS_FILE}")

# --- Authenticate GA4 API ---
try:
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    ga4_client = BetaAnalyticsDataClient(credentials=credentials)
    print("GA4 API client created.")
except Exception as e:
    print("GA4 AUTH ERROR:", str(e))
    raise

# --- Test a small GA4 request ---
try:
    test_req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
        limit=1,
    )
    test_resp = ga4_client.run_report(test_req)
    print("GA4 API test query OK. Sample data:", [
        f.value for f in test_resp.rows[0].dimension_values
    ] if test_resp.rows else "No data")
except Exception as e:
    print("GA4 TEST REQUEST ERROR:", str(e))
    raise

# --- Test connect ClickHouse ---
try:
    ch_client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
    )
    ch_client.execute("SELECT 1")
    print("ClickHouse connection: OK")
except Exception as e:
    print("ClickHouse connection failed:", str(e))
    raise

# --- Define Dimensions and Metrics ---
dimensions = [
    Dimension(name="date"),
    Dimension(name="platform"),
    Dimension(name="streamName"),
    Dimension(name="customUser:ga_session_id"),
    Dimension(name="newVsReturning"),
    Dimension(name="firstUserCampaignId"),
    Dimension(name="firstUserCampaignName"),
    Dimension(name="firstUserSourceMedium"),
    Dimension(name="fullPageUrl"),
]
metrics = [
    Metric(name="sessions"),
    Metric(name="bounceRate"),
]

# --- Filter: loại trừ streamName chứa donhang.ghn.vn và fullPageUrl chứa ghn.dev ---
filter_expression = FilterExpression(
    and_group={
        "expressions": [
            FilterExpression(
                not_expression=FilterExpression(
                    filter=Filter(
                        field_name="streamName",
                        string_filter=Filter.StringFilter(
                            value="donhang.ghn.vn",
                            match_type=Filter.StringFilter.MatchType.CONTAINS
                        )
                    )
                )
            ),
            FilterExpression(
                not_expression=FilterExpression(
                    filter=Filter(
                        field_name="fullPageUrl",
                        string_filter=Filter.StringFilter(
                            value="ghn.dev",
                            match_type=Filter.StringFilter.MatchType.CONTAINS
                        )
                    )
                )
            ),
        ]
    }
)

# --- Phân trang và lấy data ---
offset = 0
all_rows = []
page = 1
while True:
    print(f"Fetching GA4 rows: page {page}, offset {offset}")
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=dimensions,
        metrics=metrics,
        date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
        limit=MAX_ROWS,
        offset=offset,
        dimension_filter=filter_expression,
    )
    response = ga4_client.run_report(request)
    rows = []
    for row in response.rows:
        values = [f.value for f in row.dimension_values] + [f.value for f in row.metric_values]
        # Parse date sang datetime Asia/Bangkok (UTC+7)
        date_str = values[0]
        date_dt = datetime.strptime(date_str, "%Y%m%d") + timedelta(hours=7)
        values[0] = date_dt
        # sessions: UInt32, bounceRate: Float64
        values[9] = int(float(values[9]) if values[9] else 0)
        values[10] = float(values[10]) if values[10] else 0.0
        rows.append(values)
    print(f"Fetched {len(rows)} rows on page {page}.")
    all_rows.extend(rows)
    if len(rows) < MAX_ROWS:
        break
    offset += MAX_ROWS
    page += 1

print(f"===> Tổng số rows thu được từ GA4: {len(all_rows)}")

# --- Đẩy lên ClickHouse ---
if all_rows:
    try:
        insert_query = f"""
            INSERT INTO {CH_TABLE} (
                date, platform, streamName, customUser_ga_session_id, newVsReturning,
                firstUserCampaignId, firstUserCampaignName, firstUserSourceMedium, fullPageUrl,
                sessions, bounceRate
            ) VALUES
        """
        ch_client.execute(insert_query, all_rows)
        print(f"Đã insert {len(all_rows)} rows vào ClickHouse table {CH_TABLE}")
    except Exception as e:
        print("ERROR inserting data to ClickHouse:", str(e))
else:
    print("Không có dữ liệu để insert!")

print("========== Script finished ==========")
