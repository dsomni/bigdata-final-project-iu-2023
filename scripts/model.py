
from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("BDT Project")\
        .config("spark.sql.catalogImplementation","hive")\
        .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .config("spark.jars", "/usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar")\
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.4")\
        .enableHiveSupport()\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print(spark.catalog.listDatabases())

print(spark.catalog.listTables("projectdb"))

games = spark.read.format("avro").table('projectdb.games_part')
games.createOrReplaceTempView('games')

recommendations = spark.read.format("avro").table('projectdb.recommendations_part')
recommendations.createOrReplaceTempView('recommendations')

users = spark.read.format("avro").table('projectdb.users_part')
users.createOrReplaceTempView('users')

games.printSchema()
recommendations.printSchema()
users.printSchema()

spark.sql("SELECT * FROM users limit 5").show()

spark.sql("SELECT * FROM games limit 5").show()

spark.sql("SELECT * FROM recommendations limit 5").show()
