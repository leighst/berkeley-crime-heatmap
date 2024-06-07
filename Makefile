# will need to manually set latest partition
backfill:
	dagster job backfill -m berkeley_crime -j catch_up_job --from '2024-01-01' --to '2024-01-01'	

