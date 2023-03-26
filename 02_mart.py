# Databricks notebook source
spark.sql("USE CATALOG strava_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS strava_mart_schema COMMENT \"A new Unity Catalog schema called strava_mart_schema\"")

# COMMAND ----------

from pyspark.sql.functions import when, col

na_string = "N/A"

spark.sql("USE strava_stage_schema")

activity = spark.read.table("dbtestzwift_activity")

activity = (
    activity.withColumn(
        "type",
        when(col("type").isNull(), na_string).otherwise(col("type"))
    )
)
    
activity = (
    activity.withColumn(
        "sport_type",
        when(col("sport_type").isNull(), na_string).otherwise(col("sport_type"))
    )
)
    
activity = (
    activity.withColumn(
        "location_city",
        when(col("location_city").isNull(), na_string).otherwise(col("location_city"))
    )
)
    
activity = (
    activity.withColumn(
        "location_state",
        when(col("location_state").isNull(), na_string).otherwise(col("location_state"))
    )
)
    
activity = (
    activity.withColumn(
        "location_state",
        when(col("location_state").isNull(), na_string).otherwise(col("location_state"))
    )
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

spark.sql("USE strava_mart_schema")

# COMMAND ----------

dim_sport_type = (
    activity
    .select("type","sport_type")
    .distinct()
)

window = Window().orderBy("type","sport_type")
dim_sport_type = dim_sport_type.withColumn("row_number",row_number().over(window))
dim_sport_type = dim_sport_type.select(col("row_number").alias("sk_sport_type"),col("type").alias("type"),col("sport_type").alias("sport_type"))

dim_sport_type.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_dim_sport_type")

# COMMAND ----------

dim_geography = (
    activity
    .select("location_city","location_state","location_country")
    .distinct()
)

window = Window().orderBy("location_country","location_state","location_city")
dim_geography = dim_geography.withColumn("row_number",row_number().over(window))
dim_geography = dim_geography.select(col("row_number").alias("sk_geography"),col("location_city").alias("city"),col("location_state").alias("state"),col("location_country").alias("country"))

dim_geography.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_dim_geography")

# COMMAND ----------

import pandas as pd

start_date = '2009-01-01'
end_date = '2030-12-31'

date_range = pd.date_range(start=start_date, end=end_date, freq='D')

df = pd.DataFrame({'date': date_range})
df['sk_time'] = df['date'].dt.strftime('%Y%m%d')
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['day'] = df['date'].dt.day
df['date'] = df['date'].dt.date
#print(df.dtypes)
#df = df.drop('date', axis=1)

dim_time = spark.createDataFrame(df)

dim_time.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_dim_time")
# bruk .option("overwriteSchema", "true") for å lagre med endret skjema

# COMMAND ----------

from pyspark.sql.functions import year,month,dayofmonth,hour,minute,second

ftr_activity_tmp = (
    activity
    .select(
        "id",
        "distance",
        "moving_time",
        "elapsed_time",
        "total_elevation_gain",
        "comment_count",
        "athlete_count",
        "photo_count",
        "average_speed",
        "max_speed",
        "average_cadence",
        "average_temp",
        "average_heartrate",
        "max_heartrate",
        "total_photo_count",
        "suffer_score",
        "average_watts",
        "max_watts",
        "weighted_average_watts",
        "kilojoules",
        "type",
        "sport_type",
        "location_city",
        "location_state",
        "location_country",
        "start_date_local"
    )
)

ftr_activity_tmp = ftr_activity_tmp.withColumn("start_year", year("start_date_local"))
ftr_activity_tmp = ftr_activity_tmp.withColumn("start_month", month("start_date_local"))
ftr_activity_tmp = ftr_activity_tmp.withColumn("start_day", dayofmonth("start_date_local"))
ftr_activity_tmp = ftr_activity_tmp.withColumn("start_hour", hour("start_date_local"))
ftr_activity_tmp = ftr_activity_tmp.withColumn("start_minute", minute("start_date_local"))
ftr_activity_tmp = ftr_activity_tmp.withColumn("start_second", second("start_date_local"))

ftr_activity_tmp2 = ftr_activity_tmp.join(
    dim_sport_type,
    (
        (ftr_activity_tmp.type == dim_sport_type.type) &
        (ftr_activity_tmp.sport_type == dim_sport_type.sport_type)
    ),
    "inner"
)

ftr_activity_tmp3 = ftr_activity_tmp2.join(
    dim_geography,
    (
        (ftr_activity_tmp2.location_city == dim_geography.city) &
        (ftr_activity_tmp2.location_state == dim_geography.state) &
        (ftr_activity_tmp2.location_country == dim_geography.country)
    ),
    "inner"
)

ftr_activity_tmp4 = ftr_activity_tmp3.join(
    dim_time,
    (
        (ftr_activity_tmp3.start_year == dim_time.year) &
        (ftr_activity_tmp3.start_month == dim_time.month) &
        (ftr_activity_tmp3.start_day == dim_time.day)
    ),
    "inner"
)

ftr_activity = (
    ftr_activity_tmp4
    .select(
        "id",
        "distance",
        "moving_time",
        "elapsed_time",
        "total_elevation_gain",
        "comment_count",
        "athlete_count",
        "photo_count",
        "average_speed",
        "max_speed",
        "average_cadence",
        "average_temp",
        "average_heartrate",
        "max_heartrate",
        "total_photo_count",
        "suffer_score",
        "average_watts",
        "max_watts",
        "weighted_average_watts",
        "kilojoules",
        "start_hour",
        "start_minute",
        "start_second",
        "sk_sport_type",
        "sk_geography",
        "sk_time"
    )
)

ftr_activity.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_ftr_activity")

# COMMAND ----------

#spark.sql("drop table if exists dbtestzwift_dim_geography")
#spark.sql("drop table if exists dbtestzwift_dim_sport_type")
#spark.sql("drop table if exists dbtestzwift_dim_time")
#spark.sql("drop table if exists dbtestzwift_ftr_activity")
