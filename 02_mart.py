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

spark.sql("USE strava_stage_schema")

activity_zone = spark.read.table("dbtestzwift_activity_zone")
distribution_bucket = spark.read.table("dbtestzwift_distribution_bucket")

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

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("activity_id","zone_seq_no").orderBy("min")
distribution_bucket = distribution_bucket.withColumn("row_number",row_number().over(window_spec))

# COMMAND ----------

activity_zone_w_dist_bucket = activity_zone.join(
    distribution_bucket,
    [(activity_zone.activity_id == distribution_bucket.activity_id) & (activity_zone.zone_seq_no == distribution_bucket.zone_seq_no)],
    "inner"
).drop(distribution_bucket.activity_id).drop(distribution_bucket.zone_seq_no)

activity_type_bucket_count = activity_zone_w_dist_bucket.groupBy("activity_id","zone_seq_no","type").count().select("type","count").distinct().withColumnRenamed("type","zone_type").withColumnRenamed("count","bucket_count")
type_w_multiple_buck_count = activity_type_bucket_count.groupBy("zone_type").count().filter(col("count") > 1)
if type_w_multiple_buck_count.count() > 0:
    raise Exception("There are occurences of activity types with different number of buckets.")

# COMMAND ----------

activity_type_bucket_count_coll = activity_type_bucket_count.collect()
max_cols = []
sum_cols = []

for rows in activity_type_bucket_count_coll:
    zone_type = rows.zone_type
    bucket_count = rows.bucket_count
    score = "score_" + zone_type
    zone_buck_type = "zone_type_" + zone_type
    resource_state = "resource_state_" + zone_type
    sensor_based = "sensor_based_" + zone_type
    points = "points_" + zone_type
    custom_zones = "custom_zones_" + zone_type
    activity_zone_w_dist_bucket = activity_zone_w_dist_bucket \
        .withColumn(score,when(activity_zone_w_dist_bucket.type == zone_type, activity_zone_w_dist_bucket.score)) \
        .withColumn(zone_buck_type,when(activity_zone_w_dist_bucket.type == zone_type, activity_zone_w_dist_bucket.type)) \
        .withColumn(resource_state,when(activity_zone_w_dist_bucket.type == zone_type, activity_zone_w_dist_bucket.resource_state)) \
        .withColumn(sensor_based,when(activity_zone_w_dist_bucket.type == zone_type, activity_zone_w_dist_bucket.sensor_based)) \
        .withColumn(points,when(activity_zone_w_dist_bucket.type == zone_type, activity_zone_w_dist_bucket.points)) \
        .withColumn(custom_zones,when(activity_zone_w_dist_bucket.type == zone_type, activity_zone_w_dist_bucket.custom_zones))
    max_cols.append(score)
    max_cols.append(zone_buck_type)
    max_cols.append(resource_state)
    max_cols.append(sensor_based)
    max_cols.append(points)
    max_cols.append(custom_zones)
    i = 0
    while i < bucket_count:
        i = i + 1
        min_buck = "min_" + zone_type + str(i)
        max_buck = "max_" + zone_type + str(i)
        time_buck = "time_" + zone_type + str(i)
        activity_zone_w_dist_bucket = activity_zone_w_dist_bucket \
            .withColumn(min_buck, when(((activity_zone_w_dist_bucket.type == zone_type) & (activity_zone_w_dist_bucket.row_number == i)), activity_zone_w_dist_bucket.min)) \
            .withColumn(max_buck, when(((activity_zone_w_dist_bucket.type == zone_type) & (activity_zone_w_dist_bucket.row_number == i)), activity_zone_w_dist_bucket.max)) \
            .withColumn(time_buck, when(((activity_zone_w_dist_bucket.type == zone_type) & (activity_zone_w_dist_bucket.row_number == i)), activity_zone_w_dist_bucket.time))
        sum_cols.append(min_buck)
        sum_cols.append(max_buck)
        sum_cols.append(time_buck)


# COMMAND ----------

from pyspark.sql.functions import sum, max

max_cols_expr = list([max(c).alias(c) for c in max_cols])
sum_cols_expr = list([sum(c).alias(c) for c in sum_cols])

# COMMAND ----------

activity_zone_buck_agg = activity_zone_w_dist_bucket \
    .groupBy('activity_id') \
    .agg(* max_cols_expr + sum_cols_expr)
#    .agg(* sum_cols_expr)

# COMMAND ----------

activity = activity.alias('activity').join(
    activity_zone_buck_agg.alias('activity_zone_buck_agg'), 
    [(activity.id == activity_zone_buck_agg.activity_id)],
    how='leftouter'
)

# COMMAND ----------


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
        "start_date_local",
        "score_power",
        "zone_type_power",
        "resource_state_power",
        "sensor_based_power",
        "points_power",
        "custom_zones_power",
        "score_heartrate",
        "zone_type_heartrate",
        "resource_state_heartrate",
        "sensor_based_heartrate",
        "points_heartrate",
        "custom_zones_heartrate",
        "min_power1",
        "max_power1",
        "time_power1",
        "min_power2",
        "max_power2",
        "time_power2",
        "min_power3",
        "max_power3",
        "time_power3",
        "min_power4",
        "max_power4",
        "time_power4",
        "min_power5",
        "max_power5",
        "time_power5",
        "min_power6",
        "max_power6",
        "time_power6",
        "min_power7",
        "max_power7",
        "time_power7",
        "min_power8",
        "max_power8",
        "time_power8",
        "min_power9",
        "max_power9",
        "time_power9",
        "min_power10",
        "max_power10",
        "time_power10",
        "min_power11",
        "max_power11",
        "time_power11",
        "min_heartrate1",
        "max_heartrate1",
        "time_heartrate1",
        "min_heartrate2",
        "max_heartrate2",
        "time_heartrate2",
        "min_heartrate3",
        "max_heartrate3",
        "time_heartrate3",
        "min_heartrate4",
        "max_heartrate4",
        "time_heartrate4",
        "min_heartrate5",
        "max_heartrate5",
        "time_heartrate5"
    )
)

# COMMAND ----------

from pyspark.sql.functions import year,month,dayofmonth,hour,minute,second

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
        "sk_time",
        "score_power",
        "zone_type_power",
        "resource_state_power",
        "sensor_based_power",
        "points_power",
        "custom_zones_power",
        "score_heartrate",
        "zone_type_heartrate",
        "resource_state_heartrate",
        "sensor_based_heartrate",
        "points_heartrate",
        "custom_zones_heartrate",
        "min_power1",
        "max_power1",
        "time_power1",
        "min_power2",
        "max_power2",
        "time_power2",
        "min_power3",
        "max_power3",
        "time_power3",
        "min_power4",
        "max_power4",
        "time_power4",
        "min_power5",
        "max_power5",
        "time_power5",
        "min_power6",
        "max_power6",
        "time_power6",
        "min_power7",
        "max_power7",
        "time_power7",
        "min_power8",
        "max_power8",
        "time_power8",
        "min_power9",
        "max_power9",
        "time_power9",
        "min_power10",
        "max_power10",
        "time_power10",
        "min_power11",
        "max_power11",
        "time_power11",
        "min_heartrate1",
        "max_heartrate1",
        "time_heartrate1",
        "min_heartrate2",
        "max_heartrate2",
        "time_heartrate2",
        "min_heartrate3",
        "max_heartrate3",
        "time_heartrate3",
        "min_heartrate4",
        "max_heartrate4",
        "time_heartrate4",
        "min_heartrate5",
        "max_heartrate5",
        "time_heartrate5"
    )
)

#ftr_activity.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_ftr_activity")
# bruk .option("overwriteSchema", "true") for å lagre med endret skjema
ftr_activity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbtestzwift_ftr_activity")

# COMMAND ----------

#spark.sql("drop table if exists dbtestzwift_dim_geography")
#spark.sql("drop table if exists dbtestzwift_dim_sport_type")
#spark.sql("drop table if exists dbtestzwift_dim_time")
#spark.sql("drop table if exists dbtestzwift_ftr_activity")
