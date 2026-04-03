# Use this later in airflow
# from datetime import timedelta
# from pipeline.utils.watermark import read_watermark, write_watermark

# @task
# def extract_power_data():
#     last_watermark = read_watermark()

#     start = (last_watermark - timedelta(days=2)).strftime("%Y-%m-%d")

#     df = fetch_all_power_data(start=start)

#     if not df.empty:
#         write_watermark(df["timestamp"].max())

#     return len(df)
