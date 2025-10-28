import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, avg, count, desc

def main():
    spark = SparkSession.builder.appName("MovieLensAnalysis").getOrCreate()

    base_path = "/opt/spark/work-dir/"
    movies_path = os.path.join(base_path, "data/movies.dat")
    ratings_path = os.path.join(base_path, "data/ratings.dat")
    output_path = os.path.join(base_path, "analysis_results")
    os.makedirs(output_path, exist_ok=True)

    print("Loading MovieLens data...")
    movies = spark.read.option("delimiter", "::").option("inferSchema", "true") \
        .csv(movies_path, schema="movieId INT, title STRING, genres STRING")
    
    ratings = spark.read.option("delimiter", "::").option("inferSchema", "true") \
        .csv(ratings_path, schema="userId INT, movieId INT, rating DOUBLE, timestamp LONG")
    
    movies = movies.withColumn("year", 
        col("title").substr(-5, 4).cast("int"))
    
    movies.createOrReplaceTempView("movies")
    ratings.createOrReplaceTempView("ratings")
    
    print("Creating merged view...")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW merged AS
        SELECT r.*, m.title, m.genres, m.year
        FROM ratings r
        LEFT JOIN movies m ON r.movieId = m.movieId
    """)
    
    print("Data preprocessing complete.")

    print("1/9: Overview Summary - Total Movies")
    total_movies = spark.sql("SELECT COUNT(*) as total_movies FROM movies")
    total_movies.toPandas().to_json(os.path.join(output_path, "1_total_movies.json"), orient="split")

    print("2/9: Overview Summary - Total Users")
    total_users = spark.sql("SELECT COUNT(DISTINCT userId) as total_users FROM ratings")
    total_users.toPandas().to_json(os.path.join(output_path, "2_total_users.json"), orient="split")

    print("3/9: Overview Summary - Total Ratings")
    total_ratings = spark.sql("SELECT COUNT(*) as total_ratings FROM ratings")
    total_ratings.toPandas().to_json(os.path.join(output_path, "3_total_ratings.json"), orient="split")

    print("4/9: Genre Popularity - Top 15 Genres")
    genre_popularity = spark.sql("""
        SELECT genre, COUNT(*) as count
        FROM (
            SELECT explode(split(genres, '\\|')) as genre
            FROM movies
        )
        GROUP BY genre
        ORDER BY count DESC
        LIMIT 15
    """)
    genre_popularity.toPandas().to_json(os.path.join(output_path, "4_genre_popularity.json"), orient="split")

    print("5/9: Ratings Distribution")
    ratings_distribution = spark.sql("""
        SELECT rating, COUNT(*) as count
        FROM ratings
        GROUP BY rating
        ORDER BY rating
    """)
    ratings_distribution.toPandas().to_json(os.path.join(output_path, "5_ratings_distribution.json"), orient="split")

    print("6/9: Movies Released per Year - Top 10 Years")
    movies_per_year = spark.sql("""
        SELECT year, COUNT(*) as movie_count
        FROM movies
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY movie_count DESC
        LIMIT 10
    """)
    movies_per_year.toPandas().to_json(os.path.join(output_path, "6_movies_per_year_top10.json"), orient="split")

    print("7/9: Top Rated Movies per Genre (Action example)")
    top_rated_action = spark.sql("""
        SELECT title, AVG(rating) as avg_rating, COUNT(*) as rating_count
        FROM merged
        WHERE genres LIKE '%Action%'
        GROUP BY title
        HAVING rating_count >= 10
        ORDER BY avg_rating DESC
        LIMIT 10
    """)
    top_rated_action.toPandas().to_json(os.path.join(output_path, "7_top_rated_action_movies.json"), orient="split")

    print("8/9: Average Rating by Year")
    avg_rating_by_year = spark.sql("""
        SELECT year, AVG(rating) as avg_rating, COUNT(*) as rating_count
        FROM merged
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)
    avg_rating_by_year.toPandas().to_json(os.path.join(output_path, "8_avg_rating_by_year.json"), orient="split")

    print("9/9: Rating Statistics Summary")
    rating_stats = spark.sql("""
        SELECT 
            MIN(rating) as min_rating,
            MAX(rating) as max_rating,
            AVG(rating) as mean_rating,
            PERCENTILE_APPROX(rating, 0.5) as median_rating,
            COUNT(*) as total_count
        FROM ratings
    """)
    rating_stats.toPandas().to_json(os.path.join(output_path, "9_rating_statistics.json"), orient="split")

    print("All analyses complete. Results saved to", output_path)
    spark.stop()

if __name__ == "__main__":
    main()