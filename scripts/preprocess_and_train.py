import redis
import json
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator

def main():
    # --- 1. SPARK SESSION INITIALIZATION ---
    spark = SparkSession.builder \
        .appName("ALSBatchTraining") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # --- 2. LOAD DATA FROM DELTA LAKE ---
    delta_table_path = "/opt/bitnami/spark/data/delta/ratings"
    model_save_path = "/opt/bitnami/spark/models/als_model"

    print(f"Reading ratings data from Delta table at {delta_table_path}...")
    try:
        ratings_df = spark.read.format("delta").load(delta_table_path)
    except Exception as e:
        print(f"Error reading Delta table or table does not exist yet: {e}")
        print("Please ensure the Kafka consumer has run and created the table.")
        spark.stop()
        return

    # Drop any potential duplicates and rows with nulls
    ratings_df = ratings_df.dropDuplicates(['userId', 'movieId']).dropna()
    print("Successfully loaded ratings data.")
    ratings_df.show(5)

    total_count = ratings_df.count()
    print(f"Total ratings available: {total_count}")
    
    if total_count < 10:
        print(f"Warning: Only {total_count} ratings found. Need at least 10 ratings for reliable training.")
        print("Using all data for training (no test split).")
        training = ratings_df
        test = None
    else:
        (training, test) = ratings_df.randomSplit([0.8, 0.2])
    
    als = ALS(
        maxIter=5, 
        regParam=0.01, 
        userCol="userId", 
        itemCol="movieId", 
        ratingCol="rating",
        coldStartStrategy="drop"
    )

    print("Training the ALS model...")
    model = als.fit(training)
    print("Model training complete.")

    if test is not None and test.count() > 0:
        print("Evaluating model on test data...")
        predictions = model.transform(test)
        evaluator = RegressionEvaluator(
            metricName="rmse", 
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = evaluator.evaluate(predictions)
        print(f"Root-mean-square error on test data = {rmse}")
    else:
        print("Skipping evaluation (insufficient test data).")

    print(f"Saving the trained model to {model_save_path}...")
    model.write().overwrite().save(model_save_path)
    print("Model saved successfully.")

    print("Generating recommendations for all users (excluding already rated movies)...")
    
    all_users = ratings_df.select("userId").distinct()
    all_movies = ratings_df.select("movieId").distinct()
    
    user_movie_combinations = all_users.crossJoin(all_movies)
    
    existing_ratings = ratings_df.select("userId", "movieId")
    
    unrated_movies = user_movie_combinations.join(
        existing_ratings, 
        on=["userId", "movieId"], 
        how="left_anti"
    )
    
    print("Predicting ratings for unrated movies...")
    predictions = model.transform(unrated_movies)
    
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number, col
    
    window_spec = Window.partitionBy("userId").orderBy(col("prediction").desc())
    
    top_recommendations = predictions.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 10) \
        .select("userId", "movieId", "prediction")
    
    print("Recommendations generated (filtered to exclude already rated movies).")

    def save_to_redis(partition):
        r = redis.StrictRedis(host='redis', port=6379, db=0)
        for row in partition:
            user_id = row.userId
            movie_id = row.movieId
            rating = row.prediction
            
            key = f"user:{user_id}"
            existing = r.get(key)
            
            if existing:
                recs_list = json.loads(existing)
            else:
                recs_list = []
            
            recs_list.append({"movieId": int(movie_id), "rating": float(rating)})
            r.set(key, json.dumps(recs_list))

    print("Saving recommendations to Redis...")
    top_recommendations.foreachPartition(save_to_redis)
    print("Successfully saved all recommendations to Redis.")

    spark.stop()

if __name__ == "__main__":
    main()
    