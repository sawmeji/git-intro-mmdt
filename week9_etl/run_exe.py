from week9_etl_exe import transform, load

transform_df = transform()

load(transform_df)