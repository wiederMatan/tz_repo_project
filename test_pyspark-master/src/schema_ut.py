from pyspark.sql import types as t

# ==============================INFO_MAIN=========================#
INFO_MAIN_schema = t.StructType([
    t.StructField("disc_name", t.StringType(), True),
    t.StructField("now", t.TimestampType(), True),
    t.StructField("disc_total", t.IntegerType(), True),
    t.StructField("disc_used", t.IntegerType(), True),
    t.StructField("disc_free", t.IntegerType(), True),
    t.StructField("disc_type", t.StringType(), True)
    ])
# #==============================INFO_DIR=========================#
INFO_DIR_schema = t.StructType([
    t.StructField("folder_name", t.StringType(), True),
    t.StructField("folder_size", t.IntegerType(), True),
    t.StructField("fnm_with_last_change", t.StringType(), True),
    t.StructField("chk_from_date", t.TimestampType(), True),
    t.StructField("now_run", t.TimestampType(), True)
  ])

# #==============================DIR=========================#
DIR_schema = t.StructType([
    t.StructField("dirpath", t.StringType(), True),
    t.StructField("filename", t.StringType(), True),
    t.StructField("size", t.IntegerType(), True),
    t.StructField("date_modified", t.TimestampType(), True),
    t.StructField("date_created", t.TimestampType(), True)
  ])