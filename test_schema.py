import json
import os
from pprint import pprint

import pandas as pd
from fastavro import parse_schema
from fastavro.schema import load_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

schema_path = "D:\\projects\\radar-base\\radar-pipeline\\mock-data\\mock-data\\072ddb22-82ef-4b81-8460-41ab096b54bb\\android_phone_battery_level\\schema-android_phone_battery_level.json"

schema_contents = json.load(open(schema_path, "r", encoding="utf-8"))

# parsed_schema = parse_schema(schema_contents)

# print(parsed_schema)

# spark = SparkSession.builder.master("local").appName("mock").getOrCreate()

# df = spark.read.format("avro").option("avroSchema", schema_contents).load()

# struct_type = df.schema

# print(struct_type)

struct_type = StructType.fromJson(schema_contents)

print(struct_type)
