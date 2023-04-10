# Databricks notebook source
#pip install requests

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS strava_catalog")
spark.sql("USE CATALOG strava_catalog")
spark.sql("GRANT CREATE SCHEMA, CREATE TABLE, USE CATALOG ON CATALOG strava_catalog TO `account users`")
#spark.sql("GRANT CREATE SCHEMA, CREATE TABLE, CREATE VIEW, USE CATALOG ON CATALOG strava_catalog TO `account users`")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS strava_stage_schema COMMENT \"A new Unity Catalog schema called strava_stage_schema\"")
spark.sql("USE strava_stage_schema")

# COMMAND ----------

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

auth_url = "https://www.strava.com/oauth/token"
activites_url = "https://www.strava.com/api/v3/athlete/activities"
athlete_url = 'https://www.strava.com/api/v3/athlete'

payload = {
    'client_id': "78457",
    'client_secret': 'd33044b3258b6ae45e8000579cf683abd821b746',
    'refresh_token': '20dab2507bade463981b9604eca00d406af57924',
    'grant_type': "refresh_token",
    'f': 'json'
}

res = requests.post(auth_url, data=payload, verify=False)
access_token = res.json()['access_token']

header = {'Authorization': 'Bearer ' + access_token}

# COMMAND ----------

import json

my_athlete = requests.get(athlete_url, headers=header).json()

my_athlete_json = json.dumps(my_athlete)
my_athlete_json_data_list = []
my_athlete_json_data_list.append(my_athlete_json)
json_rdd = sc.parallelize(my_athlete_json_data_list)
my_athlete_df = spark.read.json(json_rdd)
type(my_athlete_df)

my_athlete_df.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_athlete")

# COMMAND ----------

request_page_num = 1
all_activities = []

while True:
    param = {'per_page': 200, 'page': request_page_num}
    my_dataset = requests.get(activites_url, headers=header, params=param).json()

    if len(my_dataset) == 0:
        print("breaking out of while loop because the response is zero, which means there must be no more activities")
        break

    if all_activities:
        print("all_activities is populated")
        all_activities.extend(my_dataset)

    else:
        print("all_activities is NOT populated")
        all_activities = my_dataset

    request_page_num += 1

my_dataset = all_activities

# COMMAND ----------

from pandas.io.json import json_normalize

# COMMAND ----------

activities = json_normalize(my_dataset)

# COMMAND ----------

# TESTING

#from pyspark.sql.types import *

#testmj_activities = activities.head(10)

#testmj_activities.display()

#cols = [
#        'id',
#    'start_date',
#    'name'
#]

#testmj_activities_2 = testmj_activities[cols]

#testmj_activities_2.display()

#testmj_schema = StructType([
#    StructField("id",LongType(),True),
#    StructField("start_date",StringType(),True),
#    StructField("name",StringType(),True)
#])

#testmj_activities_3 = spark.createDataFrame(testmj_activities_2, testmj_schema)

#testmj_activities_3.display()

# COMMAND ----------

cols = [
    'resource_state',
    'name',
    'distance',
    'moving_time',
    'elapsed_time',
    'total_elevation_gain',
    'type',
    'sport_type',
    'id',
    'start_date',
    'start_date_local',
    'timezone',
    'utc_offset',
    'location_city',
    'location_state',
    'location_country',
    'achievement_count',
    'kudos_count',
    'comment_count',
    'athlete_count',
    'photo_count',
    'trainer',
    'commute',
    'manual',
    'private',
    'visibility',
    'flagged',
    'gear_id',
    'start_latlng',
    'end_latlng',
    'average_speed',
    'max_speed',
    'average_cadence',
    'average_temp',
    'has_heartrate',
    'average_heartrate',
    'max_heartrate',
    'heartrate_opt_out',
    'display_hide_heartrate_option',
    'elev_high',
    'elev_low',
    'upload_id',
    'upload_id_str',
    'external_id',
    'from_accepted_tag',
    'pr_count',
    'total_photo_count',
    'has_kudoed',
    'suffer_score',
    'athlete.id',
    'athlete.resource_state',
    'map.id',
    'map.summary_polyline',
    'map.resource_state',
    'average_watts',
    'max_watts',
    'weighted_average_watts',
    'kilojoules',
    'device_watts',
    'workout_type'
]
activities = activities[cols]

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("resource_state",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("distance",FloatType(),True),
    StructField("moving_time",IntegerType(),True),
    StructField("elapsed_time",IntegerType(),True),
    StructField("total_elevation_gain",FloatType(),True),
    StructField("type",StringType(),True),
    StructField("sport_type",StringType(),True),
    StructField("id",LongType(),True),
    StructField("start_date",StringType(),True),
    StructField("start_date_local",StringType(),True),
    StructField("timezone",StringType(),True),
    StructField("utc_offset",FloatType(),True),
    StructField("location_city",StringType(),True),
    StructField("location_state",StringType(),True),
    StructField("location_country",StringType(),True),
    StructField("achievement_count",IntegerType(),True),
    StructField("kudos_count",IntegerType(),True),
    StructField("comment_count",IntegerType(),True),
    StructField("athlete_count",IntegerType(),True),
    StructField("photo_count",IntegerType(),True),
    StructField("trainer",BooleanType(),True),
    StructField("commute",BooleanType(),True),
    StructField("manual",BooleanType(),True),
    StructField("private",BooleanType(),True),
    StructField("visibility",StringType(),True),
    StructField("flagged",BooleanType(),True),
    StructField("gear_id",IntegerType(),True),
    StructField("start_latlng",ArrayType(FloatType(),True),True),
    StructField("end_latlng",ArrayType(FloatType(),True),True),
    StructField("average_speed",FloatType(),True),
    StructField("max_speed",FloatType(),True),
    StructField("average_cadence",FloatType(),True),
    StructField("average_temp",FloatType(),True),
    StructField("has_heartrate",BooleanType(),True),
    StructField("average_heartrate",FloatType(),True),
    StructField("max_heartrate",FloatType(),True),
    StructField("heartrate_opt_out",BooleanType(),True),
    StructField("display_hide_heartrate_option",BooleanType(),True),
    StructField("elev_high",FloatType(),True),
    StructField("elev_low",FloatType(),True),
    StructField("upload_id",LongType(),True),
    StructField("upload_id_str",StringType(),True),
    StructField("external_id",StringType(),True),
    StructField("from_accepted_tag",BooleanType(),True),
    StructField("pr_count",IntegerType(),True),
    StructField("total_photo_count",IntegerType(),True),
    StructField("has_kudoed",BooleanType(),True),
    StructField("suffer_score",FloatType(),True),
    StructField("athlete.id",LongType(),True),
    StructField("athlete.resource_state",IntegerType(),True),
    StructField("map.id",StringType(),True),
    StructField("map.summary_polyline",StringType(),True),
    StructField("map.resource_state",IntegerType(),True),
    StructField("average_watts",FloatType(),True),
    StructField("max_watts",FloatType(),True),
    StructField("weighted_average_watts",FloatType(),True),
    StructField("kilojoules",FloatType(),True),
    StructField("device_watts",BooleanType(),True),
    StructField("workout_type",FloatType(),True)
])

activities_df = spark.createDataFrame(activities, schema)

activities_df.write.format("delta").mode("overwrite").saveAsTable("dbtestzwift_activity")
# bruk .option("overwriteSchema", "true") for å lagre med endret skjema
