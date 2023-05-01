# Databricks notebook source
# init - first time

#from pyspark.sql.types import *

#spark.sql("USE strava_stage_schema")

#delta_load_status_schema = StructType([
#   StructField('id',LongType(),False),
#   StructField('activity_zones_loaded',IntegerType(),False),
#   ])

#delta_load_status = spark.createDataFrame(data = [],schema = delta_load_status_schema)

#delta_load_status.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbtestzwift_delta_load_status")
# bruk .option("overwriteSchema", "true") for å lagre med endret skjema

# COMMAND ----------

spark.sql("USE CATALOG strava_catalog")

# COMMAND ----------

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

auth_url = "https://www.strava.com/oauth/token"
activities_zones_url_first_part = "https://www.strava.com/api/v3/activities/"
activities_zones_url_last_part = "/zones"

payload = {
    'client_id': "78457",
    'client_secret': '8d858f4aecfd3132dcb4f1b66e6e1e314303cae5',
    'refresh_token': '20dab2507bade463981b9604eca00d406af57924',
    'grant_type': "refresh_token",
    'f': 'json'
}
# old: '20dab2507bade463981b9604eca00d406af57924'
# new: 'df528c432d854ed6151a77c5d888b2ae0aa28a3c'

res = requests.post(auth_url, data=payload, verify=False)
access_token = res.json()['access_token']

header = {'Authorization': 'Bearer ' + access_token}

# COMMAND ----------

spark.sql("USE strava_stage_schema")

activity = spark.read.table("dbtestzwift_activity")

# COMMAND ----------

spark.sql("USE strava_stage_schema")

activity_zone = spark.read.table("dbtestzwift_activity_zone")
activity_zone_dist_id = activity_zone.select("activity_id").distinct()

# COMMAND ----------

spark.sql("USE strava_stage_schema")

activity_wo_act_zone = spark.read.table("dbtestzwift_activity_wo_act_zone")

# COMMAND ----------

activity = activity.join(
    activity_zone_dist_id, 
    [activity.id == activity_zone_dist_id.activity_id], 
    how='left_anti'
)

# COMMAND ----------

activity = activity.join(
    activity_wo_act_zone, 
    [activity.id == activity_wo_act_zone.activity_id], 
    how='left_anti'
)

# COMMAND ----------

all_activities_zones = []
all_distribution_buckets = []
activity_wo_act_zone = []

rows_looped = activity.select("id").collect()

for rows in rows_looped:
    activity_id = rows[0]
    activities_zones_url = activities_zones_url_first_part + str(activity_id) + activities_zones_url_last_part
    response = requests.get(activities_zones_url, headers=header)
    if response != None and response.status_code == 200:
        my_dataset = response.json()
        i = 0
        while i < len(my_dataset):
            my_dataset[i]["activity_id"] = activity_id
            my_dataset[i]["zone_seq_no"] = i
            distribution_buckets = my_dataset[i]["distribution_buckets"]
            j = 0
            while j < len(distribution_buckets):
                distribution_buckets[j]["activity_id"] = activity_id
                distribution_buckets[j]["zone_seq_no"] = i
                j = j + 1
            all_distribution_buckets.extend(distribution_buckets)
            i = i + 1
        all_activities_zones.extend(my_dataset)
        if len(my_dataset) == 0:
            activity_wo_act_zone.append(activity_id)
    else:
        print(response.status_code)
        print(response)
        break

# COMMAND ----------

activity_wo_act_zone_df = spark.createDataFrame([(i,) for i in activity_wo_act_zone], ["activity_id"])

# COMMAND ----------

# initiate table

#spark.sql("USE strava_stage_schema")
#activity_wo_act_zone_df.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_activity_wo_act_zone")
#spark.sql("TRUNCATE TABLE dbtestzwift_activity_wo_act_zone")

# COMMAND ----------

from delta.tables import *

spark.sql("USE strava_stage_schema")

activity_wo_act_zone = DeltaTable.forName(spark,"dbtestzwift_activity_wo_act_zone")

activity_wo_act_zone.alias('activity_wo_act_zone') \
.merge(
    activity_wo_act_zone_df.alias('activity_wo_act_zone_df'),
   'activity_wo_act_zone.activity_id = activity_wo_act_zone_df.activity_id'
) \
.whenNotMatchedInsert(values =
{
      "activity_id": "activity_wo_act_zone_df.activity_id"
}
) \
.execute()

# COMMAND ----------

from pandas import json_normalize

activities_zones = json_normalize(all_activities_zones)

# COMMAND ----------

distribution_buckets = json_normalize(all_distribution_buckets)

# COMMAND ----------

# Kan feile hvis det bare hentes aktiviteter uten activity zones
# dbtestzwift_activity_wo_act_zone vil da oppdateres
# slik at det kan gå bedre neste runde

cols_act_zones = [
    'activity_id',
    'zone_seq_no',
    'score',
    'type',
    'resource_state',
    'sensor_based',
    'points',
    'custom_zones',
]
activities_zones = activities_zones[cols_act_zones]

# COMMAND ----------

from pyspark.sql.types import *

schema_act_zones = StructType([
    StructField("activity_id",LongType(),False),
    StructField("zone_seq_no",IntegerType(),False),
    StructField("score",IntegerType(),True),
    StructField("type",StringType(),True),
    StructField("resource_state",IntegerType(),True),
    StructField("sensor_based",BooleanType(),True),
    StructField("points",IntegerType(),True),
    StructField("custom_zones",BooleanType(),True)
])

activities_zones_df = spark.createDataFrame(activities_zones, schema_act_zones)

# COMMAND ----------

cols_dist_buckets = [
    'activity_id',
    'zone_seq_no',
    'min',
    'max',
    'time'
]
distribution_buckets = distribution_buckets[cols_dist_buckets]

# COMMAND ----------

schema_dist_buckets = StructType([
    StructField("activity_id",LongType(),False),
    StructField("zone_seq_no",IntegerType(),False),
    StructField("min",IntegerType(),True),
    StructField("max",IntegerType(),True),
    StructField("time",IntegerType(),True)
])

distribution_buckets_df = spark.createDataFrame(distribution_buckets, schema_dist_buckets)

# COMMAND ----------

# initiate table

#spark.sql("USE strava_stage_schema")
#activities_zones_df.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_activity_zone")
#spark.sql("TRUNCATE TABLE dbtestzwift_activity_zone")

# COMMAND ----------

# initiate table

#spark.sql("USE strava_stage_schema")
#distribution_buckets_df.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_distribution_bucket")
#spark.sql("TRUNCATE TABLE dbtestzwift_distribution_bucket")

# COMMAND ----------

spark.sql("USE strava_stage_schema")

activity_zone = DeltaTable.forName(spark,"dbtestzwift_activity_zone")

activity_zone.alias('activity_zone') \
.merge(
    activities_zones_df.alias('activities_zones_df'),
   'activity_zone.activity_id = activities_zones_df.activity_id AND activity_zone.zone_seq_no = activities_zones_df.zone_seq_no'
) \
.whenMatchedUpdate(set =
{
      "score": "activities_zones_df.score",
      "type": "activities_zones_df.type",
      "resource_state": "activities_zones_df.resource_state",
      "sensor_based": "activities_zones_df.sensor_based",
      "points": "activities_zones_df.points",
      "custom_zones": "activities_zones_df.custom_zones"
}
) \
.whenNotMatchedInsert(values =
{
      "activity_id": "activities_zones_df.activity_id",
      "zone_seq_no": "activities_zones_df.zone_seq_no",
      "score": "activities_zones_df.score",
      "type": "activities_zones_df.type",
      "resource_state": "activities_zones_df.resource_state",
      "sensor_based": "activities_zones_df.sensor_based",
      "points": "activities_zones_df.points",
      "custom_zones": "activities_zones_df.custom_zones"
}
) \
.execute()

# COMMAND ----------

spark.sql("USE strava_stage_schema")

distribution_bucket = DeltaTable.forName(spark,"dbtestzwift_distribution_bucket")

distribution_bucket.alias('distribution_bucket') \
.merge(
    distribution_buckets_df.alias('distribution_buckets_df'),
   'distribution_bucket.activity_id = distribution_buckets_df.activity_id AND distribution_bucket.zone_seq_no = distribution_buckets_df.zone_seq_no AND distribution_bucket.min = distribution_buckets_df.min AND distribution_bucket.max = distribution_buckets_df.max'
) \
.whenMatchedUpdate(set =
{
      "time": "distribution_buckets_df.time"
}
) \
.whenNotMatchedInsert(values =
{
      "activity_id": "distribution_buckets_df.activity_id",
      "zone_seq_no": "distribution_buckets_df.zone_seq_no",
      "min": "distribution_buckets_df.min",
      "max": "distribution_buckets_df.max",
      "time": "distribution_buckets_df.time"
}
) \
.execute()

# COMMAND ----------

# check no of rows in result tables
#spark.sql("USE strava_stage_schema")
#testmj_az = spark.read.table("dbtestzwift_activity_zone")
#testmj_db = spark.read.table("dbtestzwift_distribution_bucket")
#print(testmj_az.count())  #135 -  268 - 666 - 782 - 1152 - 1988 - 2042
#print(testmj_db.count())  #861 - 1724 - 4302 - 5062 - 7404 - 12268 - 12670
