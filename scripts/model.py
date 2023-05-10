from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("BDT Project")
    .master("local[*]")
    .config("spark.driver.memory", "15g")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")
    .config("spark.sql.avro.compression.codec", "snappy")
    .config(
        "spark.jars",
        "/usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar",
    )
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.4")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("OFF")

print(spark.catalog.listDatabases())

print(spark.catalog.listTables("projectdb"))

games = spark.read.format("avro").table("projectdb.games_part")
games.createOrReplaceTempView("games")

recommendations = spark.read.format("avro").table("projectdb.recommendations_part")
recommendations.createOrReplaceTempView("recommendations")

users = spark.read.format("avro").table("projectdb.users_part")
users.createOrReplaceTempView("users")

games.printSchema()
recommendations.printSchema()
users.printSchema()

spark.sql("SELECT * FROM users limit 5").show()

spark.sql("SELECT * FROM games limit 5").show()

spark.sql("SELECT * FROM recommendations limit 5").show()

# Choosing model

df_data = spark.sql("select * from recommendations")
bool_dict = {
    False: 1,
    True: 2,
}

encode_is_recommended = F.udf(lambda x: bool_dict[x], IntegerType())
rec_enc = df_data.withColumn(
    "is_recommended_enc", encode_is_recommended(F.col("is_recommended"))
)

# ALS model

als_data = rec_enc.drop(
    "helpful", "funny", "date_review", "hours", "review_id", "is_recommended"
)
print("ALS data")
als_data.show()

train_data, test_data = als_data.randomSplit([0.8, 0.2], seed=42)
als = ALS(
    userCol="user_id",
    itemCol="app_id",
    ratingCol="is_recommended_enc",
    seed=42,
    nonnegative=True,
    coldStartStrategy="drop",
)

als_model = als.fit(train_data)

predictions = als_model.transform(test_data)
rmse_evaluator = RegressionEvaluator(metricName="rmse", labelCol="is_recommended_enc")
rmse = rmse_evaluator.evaluate(predictions)
print("ALS default model RMSE = ", rmse)

rounded_als_pred = predictions.withColumn(
    "prediction_round", F.round(F.col("prediction")).cast("double") - 1.0
).withColumn("is_recommended_enc", F.col("is_recommended_enc").cast("double") - 1.0)

bin_evaluator = BinaryClassificationEvaluator(
    labelCol="is_recommended_enc", rawPredictionCol="prediction_round"
)

als_auc_score = bin_evaluator.evaluate(rounded_als_pred)
print("ALS AUC: ", als_auc_score)

als_scores = [("auc", float(als_auc_score)), ("rmse", float(rmse))]
als_scores_df = spark.createDataFrame(data=als_scores, schema=["score", "value"])
als_scores_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/als_scores")


als_params = [
    ("maxIter", float(als_model.getMaxIter())),
    ("regParam", float(als_model.getRegParam())),
    ("rank", float(als_model.getRank())),
]
als_params_df = spark.createDataFrame(data=als_params, schema=["parameter", "value"])
als_params_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/als_params")

# Random Forest model

rating_dict = {
    "Overwhelmingly Positive": 8,
    "Very Positive": 7,
    "Positive": 6,
    "Mostly Positive": 5,
    "Mixed": 4,
    "Mostly Negative": 3,
    "Negative": 2,
    "Very Negative": 1,
    "Overwhelmingly Negative": 0,
}

encode_rating = F.udf(lambda x: rating_dict[x], IntegerType())

df_games = (
    spark.sql("select * from games")
    .withColumn("year", F.year("date_release"))
    .drop("title", "win", "steam_deck", "price_final", "date_release")
    .withColumn("linux", F.col("linux").cast("double"))
    .withColumn("mac", F.col("mac").cast("double"))
    .withColumn("rating", encode_rating(F.col("rating")))
)

df_games_rec = df_games.join(
    rec_enc.select("is_recommended_enc", "app_id", "user_id"), "app_id", "inner"
).withColumn("is_recommended_enc", F.col("is_recommended_enc").cast("double") - 1.0)

vector_assembler = VectorAssembler(
    inputCols=[
        "app_id",
        "positive_ratio",
        "user_reviews",
        "price_original",
        "discount",
        "rating",
        "linux",
        "mac",
        "year",
        "user_id",
    ],
    outputCol="features_unscaled",
)
scaler = MinMaxScaler(inputCol="features_unscaled", outputCol="features")

pipeline = Pipeline(stages=[vector_assembler, scaler])

features_pipeline_model_svc = pipeline.fit(df_games_rec)
df_games_enc = features_pipeline_model_svc.transform(df_games_rec)

rf_data = df_games_enc.select("is_recommended_enc", "features")
print("Random Forest data")
rf_data.show()

train_rf_data, test_rf_data = rf_data.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestClassifier(labelCol="is_recommended_enc", seed=42)

rf_model = rf.fit(train_rf_data)

rf_pred = rf_model.transform(test_rf_data)

rf_auc_score = bin_evaluator.evaluate(rf_pred)
print("Random Forest AUC: ", rf_auc_score)


rf_scores = [("auc", float(rf_auc_score))]
rf_scores_df = spark.createDataFrame(data=rf_scores, schema=["score", "value"])
rf_scores_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/rf_scores")


rf_params = [
    ("numTrees", float(rf_model.getNumTrees)),
    ("maxDepth", float(rf_model.getMaxDepth())),
]
rf_params_df = spark.createDataFrame(data=rf_params, schema=["parameter", "value"])

rf_params_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/rf_params")


# Cross validation and hyperparameter tuning

als_model_cv = ALS(
    userCol="user_id",
    itemCol="app_id",
    ratingCol="is_recommended_enc",
    seed=42,
    nonnegative=True,
    coldStartStrategy="drop",
)

params = (
    ParamGridBuilder()
    .addGrid(als_model_cv.regParam, [0.05, 0.1])
    .addGrid(als_model_cv.maxIter, [5, 10])
    .build()
)


params_mapped = [dict([(y[0].name, y[1]) for y in x.items()]) for x in params]

param_names = list(params_mapped[0].keys())

cv_models = list(map(lambda x: [float(x[name]) for name in param_names], params_mapped))

cv_models_df = spark.createDataFrame(data=cv_models, schema=param_names)

cv_models_df.show()

cv_models_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/cv_models")


cv = CrossValidator(
    estimator=als_model_cv,
    estimatorParamMaps=params,
    evaluator=rmse_evaluator,
    parallelism=4,
)

best_model = cv.fit(als_data)
print("Best model regParam = ", best_model.bestModel._java_obj.parent().getRegParam())
print("Best model maxIter = ", best_model.bestModel._java_obj.parent().getMaxIter())


best_model_scores = [
    ("regParam", float(best_model.bestModel._java_obj.parent().getRegParam())),
    ("maxIter", float(best_model.bestModel._java_obj.parent().getMaxIter())),
]
best_model_scores_df = spark.createDataFrame(
    data=best_model_scores, schema=["parameter", "value"]
)
best_model_scores_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/best_model_scores")


# Final model

final_model = best_model.bestModel


def take_first(x):
    return [el[0] for el in x]


take_items = F.udf(take_first, ArrayType(IntegerType()))

final_recommendations_subset = final_model.recommendForUserSubset(
    spark.sql("select user_id from users limit 1000"), 10
).withColumn("recommendations", take_items(F.col("recommendations")))

print("Final recommendations")

final_recommendations_subset.show(truncate=False)

final_recommendations_subset.coalesce(1).write.mode("overwrite").format("json").json(
    "/project/pda/recommendations"
)
