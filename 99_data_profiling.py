# Databricks notebook source
spark.sql("USE CATALOG strava_catalog")

# COMMAND ----------

spark.sql("USE strava_stage_schema")

# COMMAND ----------

activity_zone = spark.read.table("dbtestzwift_activity_zone")
distribution_bucket = spark.read.table("dbtestzwift_distribution_bucket")

# COMMAND ----------

activity_zone.display()

# COMMAND ----------

distribution_bucket.display()

# COMMAND ----------

print(activity_zone.count())
print(distribution_bucket.count())

# COMMAND ----------

act_zone_dist_buck = activity_zone.alias('activity_zone').join(
    distribution_bucket.alias('distribution_bucket'), 
    [(activity_zone.activity_id == distribution_bucket.activity_id) & (activity_zone.zone_seq_no == distribution_bucket.zone_seq_no)],
    how='inner'
).select("activity_zone.activity_id","activity_zone.zone_seq_no","activity_zone.score","activity_zone.type","activity_zone.resource_state","activity_zone.sensor_based","activity_zone.points","activity_zone.custom_zones","distribution_bucket.min","distribution_bucket.max","distribution_bucket.time")
act_zone_dist_buck.display()

# COMMAND ----------

activity_zone_gb = activity_zone.groupBy("activity_id").count().groupBy("count").count()
activity_zone_gb.display()

# COMMAND ----------

distribution_bucket_gb = distribution_bucket.groupBy("activity_id","zone_seq_no").count().groupBy("count").count()
distribution_bucket_gb.display()

# COMMAND ----------

distribution_bucket_gb = distribution_bucket.groupBy("activity_id","zone_seq_no","type").count().groupBy("count").count()
distribution_bucket_gb.display()

# COMMAND ----------

act_zone_dist_buck_gb = act_zone_dist_buck.groupBy("activity_id","zone_seq_no","type","min","max").count().groupBy("type","min","max","count").count()
#act_zone_dist_buck_gb.display().orderBy(col("type"))
act_zone_dist_buck_gb.sort("type","min").show()
