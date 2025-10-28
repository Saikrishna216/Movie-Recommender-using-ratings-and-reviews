import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType

def main():
    # Initialize Spark Session with Delta Lake configurations
    spark = SparkSession.builder \
        .appName("KafkaDeltaConsumer") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Define the schema for the incoming rating data
    schema = StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", FloatType(), False),
        StructField("timestamp", LongType(), False),
    ])

    # Path where the Delta Lake table will be stored inside the container
    delta_table_path = "/opt/bitnami/spark/data/delta/ratings"

    # Initialize Kafka Consumer
    # Connects to the Kafka broker running in our Docker network
    consumer = KafkaConsumer(
        'movie_ratings',
        bootstrap_servers='kafka:29092', # Use the internal Docker network address
        auto_offset_reset='earliest',
        group_id='movie-ratings-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consumer started. Listening for messages on topic 'movie_ratings'...")

    # Continuously listen for new messages
    for message in consumer:
        rating_data = message.value
        print(f"Received rating: {rating_data}")

        try:
            # Convert the single JSON record into a list for Spark
            data_list = [
                (rating_data['userId'], rating_data['movieId'], rating_data['rating'], rating_data['timestamp'])
            ]

            # Create a Spark DataFrame from the new rating
            new_rating_df = spark.createDataFrame(data_list, schema=schema)

            # Write the DataFrame to the Delta Lake table in append mode
            new_rating_df.write.format("delta").mode("append").save(delta_table_path)

            print(f"Successfully appended rating for user {rating_data['userId']} to Delta Lake table.")

        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()