import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter, StringFilter
)
from google.oauth2 import service_account
from clickhouse_driver import Client
from datetime import datetime, timezone, timedelta

# === CONFIGURATION ===
START_DATE = "2025-07-01"
END_DATE = "2025-07-01"
MAX_ROWS = 30000

# ClickHouse config (set env or hardcode)
CH_HOST = os.environ.get("CH_HOST")
CH_PORT = int(os.environ.get("CH_PORT", 9000))
CH_USER = os.environ.get("CH_USER")
CH_PASSWORD = os.environ.get("CH_PASSWORD")
CH_DATABASE = os.environ.get("CH_DATABASE")
CH_TABLE = os.environ.get("CH_TABLE")
GA4_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID")

GOOGLE_CREDENTIALS_FILE = os.environ.get("GA4_CREDENTIALS_JSON")

# === 1. AUTHENTICATE ===
credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=["https://www.googleapis.com/auth/analytics.readonly"],
)
client = BetaAnalyticsDataClient(credentials=credentials)

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

# === 2. FILTER (Loại trừ 2 điều kiện) ===
filter_expression = FilterExpression(
    and_group=FilterExpression.AndGroup(
        expressions=[
            # 1. NOT streamName CONTAINS donhang.ghn.vn
            FilterExpression(
                not_expression=FilterExpression(
                    filter=Filter(
                        field_name="streamName",
                        string_filter=StringFilter(
                            value="donhang.ghn.vn",
                            match_type=StringFilter.MatchType.CONTAINS
                        )
                    )
                )
            ),
            # 2. NOT fullPageUrl CONTAINS ghn.dev
            FilterExpression(
                not_expression=FilterExpression(
                    filter=Filter(
                        field_name="fullPageUrl",
                        string_filter=StringFilter(
                            value="ghn.dev",
                            match_type=StringFilter.MatchType.CONTAINS
                        )
                    )
                )
            ),
        ]
    )
)

# === 3. PHÂN TRANG & LẤY DỮ LIỆU ===
offset = 0
all_rows = []
while True:
    print(f"Fetching rows with offset {offset} ...")
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=dimensions,
        metrics=metrics,
        date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
        limit=MAX_ROWS,
        offset=offset,
        dimension_filter=filter_expression,
    )
    response = client.run_report(request)
    rows = []
    for row in response.rows:
        # Đúng thứ tự với ClickHouse table:
        # date, platform, streamName, customUser_ga_session_id, newVsReturning,
        # firstUserCampaignId, firstUserCampaignName, firstUserSourceMedium, fullPageUrl, sessions, bounceRate

        values = [f.value for f in row.dimension_values] + [f.value for f in row.metric_values]

        # Parse date sang dạng datetime (ClickHouse dùng Asia/Bangkok = UTC+7)
        # GA4 trả ra date theo dạng 'YYYYMMDD', ví dụ '20250701'
        date_str = values[0]
        date_dt = datetime.strptime(date_str, "%Y%m%d") + timedelta(hours=7)
        values[0] = date_dt  # Chuyển sang datetime với tz +7

        # Map đúng tên cột customUser_ga_session_id
        # (nếu GA4 trả về tên khác thì rename cho đúng)
        # Không cần đổi gì vì theo dimensions đã khai báo

        # Kiểu dữ liệu
        values[9] = int(float(values[9]) if values[9] else 0)      # sessions: UInt32
        values[10] = float(values[10]) if values[10] else 0.0      # bounceRate: Float64

        rows.append(values)
    all_rows.extend(rows)
    print(f"Fetched {len(rows)} rows.")
    if len(rows) < MAX_ROWS:
        break  # Hết data rồi
    offset += MAX_ROWS

print(f"===> Tổng số rows thu được: {len(all_rows)}")

# === 4. ĐẨY LÊN CLICKHOUSE ===
if all_rows:
    ch_client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
    )
    insert_query = f"""
        INSERT INTO {CH_TABLE} (
            date, platform, streamName, customUser_ga_session_id, newVsReturning,
            firstUserCampaignId, firstUserCampaignName, firstUserSourceMedium, fullPageUrl,
            sessions, bounceRate
        ) VALUES
    """
    ch_client.execute(insert_query, all_rows)
    print(f"Đã insert {len(all_rows)} rows vào ClickHouse table {CH_TABLE}")
else:
    print("Không có dữ liệu để insert!")
