import json
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import ArrayType, StructType

# given_path = os.path.abspath(os.path.dirname(__file__))
# print("gp", given_path)
# path = os.path.dirname(given_path)
# print("path", path)


def flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias(k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df


def fetch_transformation_query_path(sql_file_path):
    path = os.path.dirname(given_path) + '\\transformation_queries\\' + sql_file_path
    print("transformation_query_path", path)
    with open(path, "r") as file:
        sql_query = file.read()

    return sql_query


def read_config(database):
    parent_path = os.path.dirname(given_path) + '\\config\\database_connection.json'
    # Read the JSON configuration file
    with open(parent_path) as f:
        config = json.load(f)[database]
    return config
