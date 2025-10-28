import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import redis
import requests
import re

st.set_page_config(
    page_title="Real-Time Movie Recommender",
    layout="wide",
    page_icon="🎬"
)

OMDB_API_KEY = "410f1631"

@st.cache_data(show_spinner=False)
def fetch_movie_poster(title, api_key):
    try:
        movie_title_search = title.split('(')[0].strip()
        url = f"http://www.omdbapi.com/?t={movie_title_search}&apikey={api_key}"
        response = requests.get(url)
        data = response.json()
        if data.get("Response") == "True" and data.get("Poster") != "N/A":
            return data["Poster"]
    except Exception:
        pass
    return "assets/placeholder.png"

@st.cache_resource
def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        return None

@st.cache_resource
def create_redis_client():
    try:
        client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
        client.ping()
        return client
    except Exception as e:
        return None

producer = create_kafka_producer()
redis_client = create_redis_client()

if redis_client is None:
    st.error("Failed to connect to Redis. Please ensure Redis is running on localhost:6379")
if producer is None:
    st.warning("Failed to connect to Kafka. Rating submission will not work.")

@st.cache_data
def load_movie_data():
    movies_df = pd.read_csv(
        "data/movies.dat", sep="::", header=None, engine="python",
        encoding="latin-1", names=["movieId", "title", "genres"]
    )
    movies_df['year'] = movies_df['title'].apply(lambda x: re.search(r'\((\d{4})\)', x).group(1) if re.search(r'\((\d{4})\)', x) else None)
    movies_df['year'] = pd.to_numeric(movies_df['year'], errors='coerce')
    return movies_df

@st.cache_data
def load_ratings_data():
    ratings_df = pd.read_csv(
        "data/ratings.dat", sep="::", header=None, engine="python",
        encoding="latin-1", names=["userId", "movieId", "rating", "timestamp"]
    )
    return ratings_df

movies_df = load_movie_data()
ratings_df = load_ratings_data()

def render_analysis_page(movies, ratings):
    st.header("Comprehensive Exploratory Data Analysis (EDA) of MovieLens Dataset")
    st.write("""
        This section provides an in-depth exploration of the MovieLens dataset — including movies, genres, ratings,
        temporal trends, and relationships between key variables.
    """)

    merged_df = pd.merge(ratings, movies, on="movieId", how="left")

    st.sidebar.header("Analysis Controls")
    st.sidebar.markdown("Use these controls to explore the data interactively.")

    numeric_cols = merged_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    categorical_cols = merged_df.select_dtypes(include=['object']).columns.tolist()

    analysis_type = st.sidebar.selectbox(
        "Select Analysis Type",
        [
            "Overview Summary",
            "Genre Popularity",
            "Ratings Distribution",
            "Movies Released per Year",
            "Top Rated Movies per Genre",
            "Average Rating by Year",
            "Rating Correlation Heatmap",
            "Custom Numeric Analysis",
            "Custom Categorical Analysis"
        ]
    )

    if analysis_type == "Overview Summary":
        st.subheader("Dataset Overview")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Movies", len(movies))
        col2.metric("Total Users", ratings['userId'].nunique())
        col3.metric("Total Ratings", len(ratings))

        st.write("### Movies Dataset Sample")
        st.dataframe(movies.head(10), use_container_width=True)

        st.write("### Ratings Dataset Sample")
        st.dataframe(ratings.head(10), use_container_width=True)

        st.write("### Ratings Summary Statistics")
        st.write(ratings['rating'].describe())

    elif analysis_type == "Genre Popularity":
        st.subheader("Movie Count by Genre")

        genres_df = movies['genres'].str.split('|', expand=True).stack().reset_index(drop=True)
        genre_counts = genres_df.value_counts().sort_values(ascending=False)

        st.bar_chart(genre_counts.head(15))
        st.write("Top 15 most common genres in the dataset.")

        if st.checkbox("Show full genre count table"):
            st.dataframe(genre_counts.reset_index().rename(columns={"index": "Genre", 0: "Count"}))

    elif analysis_type == "Ratings Distribution":
        st.subheader("Distribution of Movie Ratings")
        st.bar_chart(ratings['rating'].value_counts().sort_index())
        st.write("Distribution of user ratings (1.0 to 5.0).")

        st.write("### Histogram View")
        st.pyplot(ratings['rating'].plot(kind='hist', bins=10, title="Ratings Histogram").figure)

    elif analysis_type == "Movies Released per Year":
        st.subheader("Number of Movies Released Each Year")

        movies_per_year = movies.dropna(subset=['year']).groupby('year')['movieId'].count()
        st.line_chart(movies_per_year)

        st.write("Shows how many movies were released each year in the dataset.")

        st.write("Top 10 Most Active Years:")
        st.dataframe(movies_per_year.sort_values(ascending=False).head(10))

    elif analysis_type == "Top Rated Movies per Genre":
        st.subheader("Top Rated Movies by Genre")

        genre_choice = st.selectbox("Select Genre", sorted(set("|".join(movies['genres']).split("|"))))
        genre_movies = merged_df[merged_df['genres'].str.contains(genre_choice, na=False)]
        top_movies = genre_movies.groupby('title')['rating'].mean().sort_values(ascending=False).head(10)

        st.bar_chart(top_movies)
        st.write(f"Top 10 highest-rated movies in the *{genre_choice}* genre.")

    elif analysis_type == "Average Rating by Year":
        st.subheader("Average Rating by Movie Release Year")

        avg_rating_per_year = merged_df.dropna(subset=['year']).groupby('year')['rating'].mean()
        st.line_chart(avg_rating_per_year)
        st.write("Displays how the average movie rating changes over time.")

        if st.checkbox("Show Raw Data"):
            st.dataframe(avg_rating_per_year.reset_index().rename(columns={'rating': 'Average Rating'}))

    elif analysis_type == "Rating Correlation Heatmap":
        st.subheader("Correlation Heatmap (Numeric Features)")
        import matplotlib.pyplot as plt
        import seaborn as sns

        corr = merged_df[numeric_cols].corr()
        fig, ax = plt.subplots(figsize=(8, 5))
        sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f", ax=ax)
        st.pyplot(fig)
        st.write("Correlation between numeric variables (e.g., userId, movieId, rating, timestamp).")

    elif analysis_type == "Custom Numeric Analysis":
        st.subheader("Custom Numeric Column Analysis")

        numeric_col = st.selectbox("Select a Numeric Column", numeric_cols)
        chart_type = st.radio("Select Chart Type", ["Histogram", "Boxplot", "Line Chart"])

        if chart_type == "Histogram":
            st.pyplot(merged_df[numeric_col].plot(kind='hist', bins=20, title=f"Histogram of {numeric_col}").figure)
        elif chart_type == "Boxplot":
            import seaborn as sns
            fig, ax = plt.subplots()
            sns.boxplot(x=merged_df[numeric_col], ax=ax)
            st.pyplot(fig)
        else:
            st.line_chart(merged_df[numeric_col])

        st.write(f"Summary statistics for *{numeric_col}*:")
        st.write(merged_df[numeric_col].describe())

    elif analysis_type == "Custom Categorical Analysis":
        st.subheader("Custom Categorical Column Analysis")

        cat_col = st.selectbox("Select a Categorical Column", categorical_cols)
        top_n = st.slider("Number of Top Categories to Display", 5, 30, 10)
        counts = merged_df[cat_col].value_counts().head(top_n)

        st.bar_chart(counts)
        st.write(f"Top {top_n} values in *{cat_col}*")

        if st.checkbox("Show Full Value Counts"):
            st.dataframe(counts.reset_index().rename(columns={'index': cat_col, cat_col: 'Count'}))

st.title("Real-Time Movie Recommender")

tab1, tab2 = st.tabs(["Recommendation", "Analysis"])

with tab1:
    st.write("Select a user, find a movie, and rate it. Your rating will be sent to our system in real-time.")
    col1, col2 = st.columns([1, 1])

    with col1:
        st.header("Rate a Movie")
        user_id = st.number_input("Enter your User ID", min_value=1, max_value=9999, value=6041, step=1)
        movie_list = movies_df["title"].tolist()
        selected_movie_title = st.selectbox("Search and select a movie", options=movie_list)
        rating = st.slider("Your Rating", 1.0, 5.0, 3.0, 0.5)

        if st.button("Submit Rating"):
            if producer:
                movie_id = movies_df[movies_df['title'] == selected_movie_title]['movieId'].iloc[0]
                rating_data = { 'userId': int(user_id), 'movieId': int(movie_id), 'rating': float(rating), 'timestamp': int(time.time()) }
                producer.send('movie_ratings', value=rating_data)
                producer.flush()
                st.success(f"Sent rating: {rating} for '{selected_movie_title}' by User {user_id}!")
            else:
                st.error("Cannot send rating: Kafka producer is not available.")

    with col2:
        st.header(f"Recommendations for User {user_id}")
        if redis_client:
            redis_key = f"user:{user_id}"
            recommendations_json = redis_client.get(redis_key)
            if recommendations_json:
                recs_data = json.loads(recommendations_json)
                recommended_movie_ids = [rec['movieId'] for rec in recs_data]
                recommended_movies_df = movies_df[movies_df['movieId'].isin(recommended_movie_ids)]
                num_cols = 4
                cols = st.columns(num_cols)
                for index, row in enumerate(recommended_movies_df.itertuples()):
                    with cols[index % num_cols]:
                        poster_url = fetch_movie_poster(row.title, OMDB_API_KEY)
                        st.image(poster_url, caption=row.title, use_column_width=True)
            else:
                st.info("No recommendations found for this user. Please rate some movies and ensure the batch training job has been run.")
        else:
            st.error("Cannot fetch recommendations: Redis client is not available.")

with tab2:
    render_analysis_page(movies_df, ratings_df)