from dagster import define_asset_job, DailyPartitionsDefinition
from .assets import partitions_def

catch_up_job = define_asset_job(
  "catch_up_job",
  partitions_def = partitions_def,
  config={
    "execution": {
      "config": {
        "multiprocess": {
          "max_concurrent": 1,      # limits concurrent assets to 3
        }
      }
    }
  }
)
