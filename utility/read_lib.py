import json
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


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
    if type == 'json':
        if multiline == 'TRUE':
            df = spark.read.format("json").option("multiline", True).load(path)
            return df
        else:
            df = spark.read.format("json").option("multiline", False).load(path)
            return df
