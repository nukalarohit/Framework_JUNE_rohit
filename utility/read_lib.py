import json
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession

from utility.general_utility import flatten, fetch_transformation_query_path, read_config


# spark = SparkSession.builder.getOrCreate()


def read_file(path, type, spark, schema, multiline):
    if type == 'csv':
        if schema != 'NOT APPL':
            with open(schema, 'r') as schema_file:
                schema = StructType.fromJson(json.load(schema_file))
            df = spark.read.schema(schema).option("header", True).option("delimiter", ",").csv(path)
            return df
        else:
            df = spark.read.csv(path, header=True, inferSchema=True)
            return df
    elif type == 'json':
        if multiline == 'yes':
            df = spark.read.format("json").option("multiline", True).load(path)
            df = flatten(df)
            return df
        else:
            df = spark.read.format("json").option("multiline", False).load(path)
            return df
    elif type == 'parquet':
        df = spark.read.format("parquet").load(path)
        return df
    elif type == 'text':
        df = spark.read.text(path)
        return df


def read_db(spark, database, transformation_query_path):
    with open(r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\config\config.json") as f:
        config_data = json.load(f)[database]
    with open(transformation_query_path, "r") as file:
        sql_query = file.read()

    df = spark.read.format("jdbc"). \
        option("url", config_data['url']). \
        option("user", config_data['user']). \
        option("password", config_data['password']). \
        option("query", sql_query). \
        option("driver", config_data['driver']).load()
    return df


