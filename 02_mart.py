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

# OBS - fortsett her....

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

#print(activity_zone.count())
#activity_zone.show()
#print(distribution_bucket.count())
#distribution_bucket.show()
#act_zone_dist_buck_gb.sort("type","min").show()







# COMMAND ----------

window_spec = Window.partitionBy("activity_id","zone_seq_no").orderBy("min")
distribution_bucket = distribution_bucket.withColumn("row_number",row_number().over(window_spec))

# COMMAND ----------

#activity_zone.show()

# join activity_zone og distribution_bucket
# finn antall rader per activity_id, zone_seq_no, type
# finn unike forekomster av type og antall
# meld feil hvis det finnes mer enn 1 rad for 1 eller flere typer
# hvis ikke har man antall buckets per type - antakeligvis heartrate 5 og power 11

# iterer over activity + buckets per type - iterator-teller i
# opprett kolonne max_[i] og time_[i] (min også?)
# når row_number = i sett max_[i] = max og time_[i] = time
# opprett 1 tabell per type
# i disse summeres max_[i] og time_[i] per activity_id
# disse tabellen kan joines med activity og inkluderes i faktatabellen



activity_zone_w_dist_bucket = activity_zone.join(
    distribution_bucket,
    [(activity_zone.activity_id == distribution_bucket.activity_id) & (activity_zone.zone_seq_no == distribution_bucket.zone_seq_no)],
    "inner"
).drop(distribution_bucket.activity_id).drop(distribution_bucket.zone_seq_no)

activity_type_bucket_count = activity_zone_w_dist_bucket.groupBy("activity_id","zone_seq_no","type").count().select("type","count").distinct()
type_w_multiple_buck_count = activity_type_bucket_count.groupBy("type").count().filter(col("count") > 1)
if type_w_multiple_buck_count.count() > 0:
    raise Exception("There occurences of activity types with different number of buckets.")

# COMMAND ----------

# NB - fjerne denne

from pyspark.sql.functions import first
from pandas import json_normalize

activity_zone_buck = []

activity_zone_w_dist_bucket_collect = activity_zone_w_dist_bucket.collect()
i = 0

for rows in activity_zone_w_dist_bucket_collect:
    activity_type = rows.type
    no_of_buck_df = activity_type_bucket_count.select("count").filter(activity_type_bucket_count.type == activity_type)
    no_of_buck = no_of_buck_df.collect()[0][0]
    activity_dict = rows.asDict()
    min_buck = "min_" + str(rows.row_number)
    max_buck = "max_" + str(rows.row_number)
    time_buck = "time_" + str(rows.row_number)
    activity_dict[min_buck] = rows.min
    activity_dict[max_buck] = rows.max
    activity_dict[time_buck] = rows.time
    activity_zone_buck.append(activity_dict)
    #i = i + 1
    #if i > 50:
    #    break

activity_zone_buck_df = json_normalize(activity_zone_buck)
#display(test_df)

# COMMAND ----------

from pyspark.sql.functions import max

activity_type_bucket_count.display()
max_no_of_buck = activity_type_bucket_count.select(max('count')).collect()[0][0]
print(max_no_of_buck)

# COMMAND ----------

sum_cols = []
i = 0
while i < max_no_of_buck:
    i = i + 1
    min_buck = "min_" + str(i)
    max_buck = "max_" + str(i)
    time_buck = "time_" + str(i)
    activity_zone_w_dist_bucket = activity_zone_w_dist_bucket \
        .withColumn(min_buck, when(activity_zone_w_dist_bucket.row_number == i, activity_zone_w_dist_bucket.min)) \
        .withColumn(max_buck, when(activity_zone_w_dist_bucket.row_number == i, activity_zone_w_dist_bucket.max)) \
        .withColumn(time_buck, when(activity_zone_w_dist_bucket.row_number == i, activity_zone_w_dist_bucket.time))
    sum_cols.append(min_buck)
    sum_cols.append(max_buck)
    sum_cols.append(time_buck)

# COMMAND ----------

print(sum_cols)

# COMMAND ----------

from pyspark.sql.functions import sum

sum_cols_expr = list([sum(c).alias(c) for c in sum_cols])

# COMMAND ----------

activity_zone_buck_agg = activity_zone_w_dist_bucket \
    .groupBy('activity_id','zone_seq_no','score','type','resource_state','sensor_based','points','custom_zones') \
    .agg(* sum_cols_expr)

# COMMAND ----------

activity_zone_buck_agg.display()

# COMMAND ----------

act_2 = activity.alias('activity').join(
    activity_zone_buck_agg.alias('activity_zone_buck_agg'), 
    [(activity.id == activity_zone_buck_agg.activity_id)],
    how='leftouter'
)

# COMMAND ----------

print(activity.count())
print(activity_zone_buck_agg.count())
print(act_2.count())

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
# bruk .option("overwriteSchema", "true") for å lagre med endret skjema

# COMMAND ----------

#spark.sql("drop table if exists dbtestzwift_dim_geography")
#spark.sql("drop table if exists dbtestzwift_dim_sport_type")
#spark.sql("drop table if exists dbtestzwift_dim_time")
#spark.sql("drop table if exists dbtestzwift_ftr_activity")
