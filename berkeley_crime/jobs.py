from dagster import define_asset_job, DailyPartitionsDefinition

start_date = "2024-01-01+0000"
end_date = "2024-01-02+0000"

catch_up_job = define_asset_job(
  "catch_up_job",
  partitions_def = DailyPartitionsDefinition(
    start_date=start_date, end_date=end_date
  )
)
