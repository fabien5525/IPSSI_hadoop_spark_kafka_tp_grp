#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import time

# Creer la session Spark
spark = SparkSession.builder.appName("test").getOrCreate()

# Mute les logs inferieur au niveau Warning
spark.sparkContext.setLogLevel("WARN")

# Recuperation de mes data de mon stream kafka
rd = spark.readStream
rd = rd.option("kafka.bootstrap.servers", 'kafka:9092')
rd = rd.option("subscribe", "topic1")
rd = rd.format("kafka")
df = rd.load()

# Caster les data de mon string pour les rendre utilisables
df = df.selectExpr("CAST(value AS STRING)")

#conevrt the data to json {"timestamp": "2024-03-24T06:49:27", "achat": 50642.69, "vente": 50643.41, "volume": 25}
query = df.selectExpr("from_json(value, 'timestamp STRING, achat STRING, vente STRING, volume STRING') as value")

# HDFS
hdfs_path = 'hdfs://namenode:9000/data'

# faire du traitement sur les data (rajouter le delta entre 'achat' et 'vente')
query.withColumn("delta", expr("CAST(value.achat AS INT) - CAST(value.vente AS INT)"))

# Afficher les data
query.writeStream.outputMode("append").format("console").start()

# sauvegarder la data dans hdfs
statement = query.writeStream.format("json").outputMode("append").option("path", hdfs_path + '/' + str(time.time())).option("checkpointLocation", "checkpoint").start()

statement.awaitTermination()