import logging, traceback
import requests
import sys

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    IntegerType,
    DateType,
    StructField,
    StringType,
    TimestampType,
)


starting_year = int(sys.argv[1])
ending_year = int(sys.argv[2])


spark = SparkSession.builder.master("yarn").appName("noaa_climatology").getOrCreate()


bucket = "climatology_raw"
spark.conf.set("persistentGcsBucket", bucket)


for year in range(starting_year, ending_year + 1):

    df_aux = spark.read.parquet(f"gs://climatology_raw/{year}.parquet")

    df_aux = (
        df_aux.drop("flag_q")
        .drop("flag_m")
        .drop("flag_s")
        .withColumn(
            "temp_max",
            F.when(
                df_aux.meteorological_element == "TMAX",
                df_aux.data_value.cast("double") / 10,
            ).otherwise(None),
        )
        .withColumn(
            "temp_min",
            F.when(
                df_aux.meteorological_element == "TMIN",
                df_aux.data_value.cast("double") / 10,
            ).otherwise(None),
        )
        .withColumn(
            "temp_avg",
            F.when(
                df_aux.meteorological_element == "TAVG",
                df_aux.data_value.cast("double") / 10,
            ).otherwise(None),
        )
        .withColumn(
            "precipitation",
            F.when(
                df_aux.meteorological_element == "PRCP",
                df_aux.data_value.cast("double") / 10,
            ).otherwise(None),
        )
    )
    if year == starting_year:
        df = df_aux
    else:
        df = df.union(df_aux)

stations_df = (
    spark.read.parquet("gs://climatology_raw/ghcnd-stations.parquet")
    .withColumnRenamed("name", "station_name")
    .withColumnRenamed("id", "station_id")
    .withColumn("latitude_longitude", F.concat("latitude", F.lit(", "), "longitude"))
    .withColumn("country_code", F.substring("station_id", 0, 2))
    .drop(
        "state",
        "gsn_flag",
        "hcn_crn_flag",
        "wmo_id",
        "latitude",
        "longitude",
        "elevation",
    )
)

countries_df = spark.read.parquet(
    "gs://climatology_raw/ghcnd-countries.parquet"
).withColumnRenamed("name", "country_name")

stations_daily_df = (
    df.groupBy("id", "date")
    .agg(
        F.round(F.avg("temp_max"), 1),
        F.round(F.avg("temp_min"), 1),
        F.round(F.avg("temp_avg"), 1),
        F.round(F.avg("precipitation"), 1),
    )
    .join(stations_df, df.id == stations_df.station_id, "inner")
    .drop("station_id", "country_code")
    .toDF(
        "id",
        "date",
        "temp_max",
        "temp_min",
        "temp_avg",
        "precipitation",
        "station_name",
        "latitude_longitude",
    )
)

stations_monthly_df = (
    df.withColumn("date", F.trunc("date", "MM"))
    .groupBy("id", "date")
    .agg(
        F.round(F.max("temp_max_abs"), 1),
        F.round(F.avg("temp_max_avg"), 1),
        F.round(F.avg("temp_avg_avg"), 1),
        F.round(F.avg("temp_min_avg"), 1),
        F.round(F.min("temp_min_abs"), 1),
        F.round(F.sum("precipitation"), 1),
        F.count(F.when(F.col("precipitation") > 1, 1)),
    )
    .join(stations_df, df.id == stations_df.station_id, "inner")
    .drop("station_id", "country_code")
    .toDF(
        "id",
        "date",
        "temp_max_abs",
        "temp_max_avg",
        "temp_avg_avg",
        "temp_min_avg",
        "temp_min_abs",
        "precipitation_total",
        "precipitation_days",
        "station_name",
        "latitude_longitude",
    )
)
stations_yearly_df = (
    df.withColumn("date", F.trunc("date", "year"))
    .groupBy("id", "date")
    .agg(F.round(F.avg("temp_avg"), 1),)
    .join(stations_df, df.id == stations_df.station_id, "inner")
    .drop("station_id", "country_code")
    .toDF("id", "date", "temp_avg", "station_name", "latitude_longitude")
)


stations_daily_df.write.format("bigquery").mode("overwrite").option(
    "partitionField", "date"
).option("partitionType", "YEAR").option("clusteredFields", "id").option(
    "project", "global-maxim-9999"
).option(
    "dataset", "production"
).option(
    "table", "spark_stations_daily"
).save()


stations_monthly_df.write.format("bigquery").mode("overwrite").option(
    "partitionField", "date"
).option("partitionType", "YEAR").option("clusteredFields", "id").option(
    "project", "global-maxim-9999"
).option(
    "dataset", "production"
).option(
    "table", "spark_stations_monthly"
).save()


stations_yearly_df.write.format("bigquery").mode("overwrite").option(
    "clusteredFields", "date"
).option("project", "global-maxim-9999").option("dataset", "production").option(
    "table", "spark_stations_yearly"
).save()

countries_yearly_df = (
    df.withColumn("date", F.trunc("date", "year"))
    .join(stations_df, df.id == stations_df.station_id, "inner")
    .join(countries_df, stations_df.country_code == countries_df.code, "inner")
    .groupBy("country_name", "date")
    .agg(F.round(F.avg("temp_avg"), 1),)
    .toDF("country_name", "date", "temp_avg")
)
countries_historic_df = (
    df.withColumn("date", F.trunc("date", "year"))
    .join(stations_df, df.id == stations_df.station_id, "inner")
    .join(countries_df, stations_df.country_code == countries_df.code, "inner")
    .groupBy("country_name")
    .agg(
        F.round(F.max("temp_max"), 1),
        F.round(F.avg("temp_avg"), 1),
        F.round(F.min("temp_min"), 1),
    )
    .toDF("country_name", "temp_max", "temp_avg", "temp_min")
)
countries_yearly_df.write.format("bigquery").mode("overwrite").option(
    "clusteredFields", "date, country_name"
).option("project", "global-maxim-9999").option("dataset", "production").option(
    "table", "spark_countries_yearly"
).save()

countries_historic_df.write.format("bigquery").mode("overwrite").option(
    "clusteredFields", "date, country_name"
).option("project", "global-maxim-9999").option("dataset", "production").option(
    "table", "spark_countries_historic"
).save()
