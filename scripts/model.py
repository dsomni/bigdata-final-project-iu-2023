from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, ArrayType, DoubleType
from pyspark.ml.evaluation import RegressionEvaluator, RankingEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import pyspark.sql.functions as F


SAVE_LIMIT = 100


spark = (
    SparkSession.builder.appName("BD Project")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
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


# Data preparation
df_data = spark.sql("select * from recommendations")

bool_dict = {
    False: 1,
    True: 2,
}

encode_is_recommended = F.udf(lambda x: bool_dict[x], IntegerType())
rec_enc = df_data.withColumn(
    "is_recommended_enc", encode_is_recommended(F.col("is_recommended"))
)

# Evaluation functions

map_evaluator = RankingEvaluator(
    labelCol="initial_recommendations",
    predictionCol="prediction_recommendations",
    metricName="meanAveragePrecision",
)
ndcg_evaluator = RankingEvaluator(
    labelCol="initial_recommendations",
    predictionCol="prediction_recommendations",
    metricName="ndcgAtK",
    k=5,
)


def get_preferences_list(s, k=5):
    without_unliked = [el for el in s if el[1] > 1]

    res = [ell[0] for ell in sorted(without_unliked, key=lambda x: x[1], reverse=True)]

    if k is None:
        return res
    return res[:k]


preferencesListUDF = F.udf(
    lambda x: get_preferences_list(x, None), ArrayType(DoubleType())
)
fivePreferencesListUDF = F.udf(
    lambda x: get_preferences_list(x, 5), ArrayType(DoubleType())
)

takeFirstFiveUDF = F.udf(lambda x: x[:5], ArrayType(DoubleType()))


def get_preferences(df, in_column="", out_column="", custom_udf=preferencesListUDF):
    return (
        df.groupBy("user_id")
        .agg(F.collect_list(F.array("app_id", F.col(in_column))).alias(out_column))
        .withColumn(out_column, custom_udf(F.col(out_column)))
    )


def get_preference_data(prediction_df):
    initial_recs = get_preferences(
        prediction_df,
        in_column="is_recommended_enc",
        out_column="initial_recommendations",
    )
    predicted_recs = get_preferences(
        prediction_df,
        in_column="prediction",
        out_column="prediction_recommendations",
        custom_udf=fivePreferencesListUDF,
    )

    transformed_data = initial_recs.join(predicted_recs, "user_id", "inner")
    return transformed_data


def evaluate_recommendations(prediction_df, model_name=""):
    transformed_data = get_preference_data(prediction_df)

    print("Start evaluating {} MAP...".format(model_name))
    map_score = map_evaluator.evaluate(transformed_data)
    print("Finish with {} MAP".format(model_name))

    print("Start evaluating {} NDCG...".format(model_name))
    ndcg_score = ndcg_evaluator.evaluate(transformed_data)
    print("Finish with {} NDCG".format(model_name))

    print("{} MAP score = {:.3f}".format(model_name, map_score))
    print("{} NDCG score = {:.3f}".format(model_name, ndcg_score))

    return transformed_data, map_score, ndcg_score


# ALS model

als_data = rec_enc.drop(
    "helpful", "funny", "date_review", "hours", "review_id", "is_recommended"
)
print("ALS data")
als_data.show()

## Cross-validation

rmse_evaluator = RegressionEvaluator(metricName="rmse", labelCol="is_recommended_enc")
als_train_data, als_test_data = als_data.randomSplit([0.7, 0.3], seed=42)

als_model = ALS(
    userCol="user_id",
    itemCol="app_id",
    ratingCol="is_recommended_enc",
    seed=42,
    nonnegative=True,
    coldStartStrategy="drop",
)
als_params = (
    ParamGridBuilder()
    .addGrid(als_model.regParam, [0.05, 0.1])
    .addGrid(als_model.rank, [5, 10])
    .build()
)
cv_als = CrossValidator(
    estimator=als_model,
    estimatorParamMaps=als_params,
    evaluator=rmse_evaluator,
    parallelism=2,
    numFolds=2,
    seed=42,
)
cv_als_model = cv_als.fit(als_train_data)



als_params_mapped = [ dict([(y_als[0].name, y_als[1]) for y_als in x_als.items()]) for x_als in als_params ]
als_param_names = list(als_params_mapped[0].keys())
cv_als_config = [ [float(x[name]) for name in als_param_names] for x in als_params_mapped]
cv_als_config_df = spark.createDataFrame(data=cv_als_config, schema=als_param_names)
cv_als_config_df.show()
cv_als_config_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/cv_als_config")


best_als_regParam = cv_als_model.bestModel._java_obj.parent().getRegParam()
best_als_rank = cv_als_model.bestModel._java_obj.parent().getRank()

print("Best ALS model regParam = ", best_als_regParam)
print("Best ALS model rank = ", best_als_rank)

best_als_params = [
    ("regParam", float(best_als_regParam)),
    ("rank", float(best_als_rank)),
]
best_als_params_df = spark.createDataFrame(
    data=best_als_params, schema=["parameter", "value"]
)
best_als_params_df.show()
best_als_params_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/best_als_params")

final_als = cv_als_model.bestModel
final_als.save("/project/models/als")

## Testing

als_predictions = final_als.transform(als_test_data)
als_predictions.show()

als_recommendations, als_map_score, als_ndcg_score = evaluate_recommendations(
    als_predictions.withColumn(
        "is_recommended_enc", F.col("is_recommended_enc").cast("double")
    ),
    "ALS",
)


best_als_scores = [("MAP", float(als_map_score)), ("NDCG", float(als_ndcg_score))]
best_als_scores_df = spark.createDataFrame(
    data=best_als_scores, schema=["metric", "value"]
)
best_als_scores_df.show()
best_als_scores_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/best_als_scores")


als_recommendations.limit(SAVE_LIMIT).coalesce(1).write.mode("overwrite").format(
    "json"
).json("/project/pda/als_recommendations")

# Random Forest model


## Features encoding
df_games = spark.sql("select * from games")
df_games = df_games.withColumn("year", F.year("date_release"))
df_games = df_games.drop("title", "steam_deck", "price_final", "date_release")
df_games = (
    df_games.withColumn("linux", F.col("linux").cast("double"))
    .withColumn("mac", F.col("mac").cast("double"))
    .withColumn("win", F.col("win").cast("double"))
)

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
df_games = df_games.withColumn("rating", encode_rating(F.col("rating")))
df_games_rec = df_games.join(
    rec_enc.select("is_recommended_enc", "app_id", "user_id"), "app_id", "inner"
)
df_games_rec = df_games_rec.withColumn(
    "is_recommended_enc", F.col("is_recommended_enc").cast("double") - 1.0
)


feature_columns_rf = [
    "app_id",
    "positive_ratio",
    "user_reviews",
    "price_original",
    "discount",
    "rating",
    "linux",
    "mac",
    "win",
    "year",
    "user_id",
]
vector_assembler = VectorAssembler(
    inputCols=feature_columns_rf, outputCol="features_unscaled"
)
scaler = MinMaxScaler(inputCol="features_unscaled", outputCol="features")
pipeline = Pipeline(stages=[vector_assembler, scaler])
features_pipeline_model_svc = pipeline.fit(df_games_rec)
df_games_enc = features_pipeline_model_svc.transform(df_games_rec)
df_games_enc.show()

rf_features = [(c,) for c in feature_columns_rf]
rf_features_df = spark.createDataFrame(data=rf_features, schema=["feature"])
rf_features_df.show()
rf_features_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/rf_features")


rf_data = df_games_enc.select("user_id", "app_id", "is_recommended_enc", "features")
rf_data.show()

## Cross-validation
rf_train_data, rf_test_data = rf_data.randomSplit([0.7, 0.3], seed=42)

rf_model = RandomForestClassifier(labelCol="is_recommended_enc", seed=42)
rf_params = (
    ParamGridBuilder()
    .addGrid(rf_model.numTrees, [5, 10])
    .addGrid(rf_model.maxDepth, [3, 5])
    .build()
)
cv_rf = CrossValidator(
    estimator=rf_model,
    estimatorParamMaps=rf_params,
    evaluator=rmse_evaluator,
    parallelism=2,
    numFolds=2,
    seed=42,
)
cv_rf_model = cv_rf.fit(rf_train_data)



rf_params_mapped = [ dict([(y_rf[0].name, y_rf[1]) for y_rf in x_rf.items()]) for x_rf in rf_params ]
rf_param_names = list(rf_params_mapped[0].keys())
cv_rf_config = [ [float(x[name]) for name in rf_param_names] for x in rf_params_mapped]
cv_rf_config_df = spark.createDataFrame(data=cv_rf_config, schema=rf_param_names)
cv_rf_config_df.show()
cv_rf_config_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/cv_rf_config")


best_rf_numTrees = cv_rf_model.bestModel._java_obj.parent().getNumTrees()
best_rf_maxDepth = cv_rf_model.bestModel._java_obj.parent().getMaxDepth()
print("Best RF model numTrees = ", best_rf_numTrees)
print("Best RF model maxDepth = ", best_rf_maxDepth)
best_rf_params = [
    ("numTrees", float(best_rf_numTrees)),
    ("maxDepth", float(best_rf_maxDepth)),
]
best_rf_params_df = spark.createDataFrame(
    data=best_rf_params, schema=["parameter", "value"]
)
best_rf_params_df.show()
best_rf_params_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/best_rf_params")


final_rf = cv_rf_model.bestModel
final_rf.save("/project/models/rf")

## Testing

rf_predictions = (
    final_rf.transform(rf_test_data)
    .select("user_id", "app_id", "is_recommended_enc", "prediction")
    .withColumn("is_recommended_enc", F.col("is_recommended_enc") + 1)
    .withColumn("prediction", F.col("prediction") + 1)
)
rf_predictions.show()


rf_recommendations, rf_map_score, rf_ndcg_score = evaluate_recommendations(
    rf_predictions.withColumn(
        "is_recommended_enc", F.col("is_recommended_enc").cast("double")
    ),
    "RF",
)

best_rf_scores = [("MAP", float(rf_map_score)), ("NDCG", float(rf_ndcg_score))]
best_rf_scores_df = spark.createDataFrame(
    data=best_rf_scores, schema=["metric", "value"]
)
best_rf_scores_df.show()
best_rf_scores_df.coalesce(1).write.mode("overwrite").format("csv").option(
    "sep", ","
).option("header", "true").csv("/project/pda/best_rf_scores")


rf_recommendations.limit(SAVE_LIMIT).coalesce(1).write.mode("overwrite").format(
    "json"
).json("/project/pda/rf_recommendations")
