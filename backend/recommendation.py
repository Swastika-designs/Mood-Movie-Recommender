import sqlite3
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.cluster import KMeans

# Connect to the warehouse
conn = sqlite3.connect('database/warehouse.db')

query = """
SELECT f.user_id, f.movie_id, mo.title AS movie_title, mo.genre, mo.release_year, m.mood, a.activity
FROM UserMovieInteraction f
JOIN Mood m ON f.mood_id = m.mood_id
JOIN Activity a ON f.activity_id = a.activity_id
JOIN Movies mo ON f.movie_id = mo.movie_id
"""
df = pd.read_sql(query, conn)
conn.close()

# Encode categorical variables for clustering
le_mood = LabelEncoder()
le_activity = LabelEncoder()
df['mood_enc'] = le_mood.fit_transform(df['mood'])
df['activity_enc'] = le_activity.fit_transform(df['activity'])

# Cluster users by mood + activity
X = df[['mood_enc', 'activity_enc']]
n_clusters = min(4, len(df))
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
df['cluster'] = kmeans.fit_predict(X)

# Recommendation function
def recommend_movies(input_mood, input_activity, top_n=5):
    mood_enc = le_mood.transform([input_mood])[0]
    activity_enc = le_activity.transform([input_activity])[0]

    X_input = pd.DataFrame([[mood_enc, activity_enc]], columns=['mood_enc','activity_enc'])
    cluster_id = kmeans.predict(X_input)[0]

    cluster_users = df[(df['cluster']==cluster_id) &
                       (df['mood']==input_mood) &
                       (df['activity']==input_activity)]

    if not cluster_users.empty:
        top_movies = cluster_users.groupby('movie_title')['movie_id'].count() \
                                  .sort_values(ascending=False).head(top_n).index.tolist()
        return top_movies
    else:
        # fallback: most popular movies overall
        top_movies = df.groupby('movie_title')['movie_id'].count().sort_values(ascending=False).head(top_n).index.tolist()
        return top_movies
