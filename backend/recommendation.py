import sqlite3
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.cluster import KMeans
import random
import numpy as np

DB_PATH = 'database/warehouse.db'


# --------------------------------------------------------------------
# LOAD DATA
# --------------------------------------------------------------------
def load_data():
    conn = sqlite3.connect(DB_PATH)

    query = """
    SELECT 
        f.user_id,
        f.movie_id,
        mo.title AS movie_title,
        mo.genre,
        mo.release_year,
        mo.Director,
        mo.Starring,
        mo.Synopsis,
        m.mood,
        a.activity,
        f.rating
    FROM UserMovieInteraction f
    JOIN Mood m ON f.mood_id = m.mood_id
    JOIN Activity a ON f.activity_id = a.activity_id
    JOIN Movies mo ON f.movie_id = mo.movie_id
    """
    try:
        df_interactions = pd.read_sql(query, conn)
        conn.close()
        return df_interactions
    except Exception as e:
        print(f"Database Load Error: {e}")
        conn.close()
        return pd.DataFrame()


df = load_data()



# --------------------------------------------------------------------
# CLUSTER SETUP
# --------------------------------------------------------------------
if not df.empty and 'mood' in df and 'activity' in df:
    try:
        le_mood = LabelEncoder()
        le_activity = LabelEncoder()

        df['mood_enc'] = le_mood.fit_transform(df['mood'])
        df['activity_enc'] = le_activity.fit_transform(df['activity'])

        X = df[['mood_enc', 'activity_enc']]
        n_clusters = max(2, min(4, len(df)))

        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
        df['cluster'] = kmeans.fit_predict(X)

    except Exception as e:
        print("Clustering failed:", e)
        df['cluster'] = 0
else:
    df['cluster'] = 0



# --------------------------------------------------------------------
# RECOMMENDATION FUNCTION
# --------------------------------------------------------------------
def recommend_movies(input_mood, input_activity, top_n=5):

    if df.empty:
        return ["Error: No data loaded."]

    try:
        mood_enc = le_mood.transform([input_mood])[0]
        activity_enc = le_activity.transform([input_activity])[0]
    except:
        # unseen mood or activity
        mood_enc = df['mood_enc'].mode()[0]
        activity_enc = df['activity_enc'].mode()[0]

    try:
        cluster_id = kmeans.predict([[mood_enc, activity_enc]])[0]
    except:
        cluster_id = 0

    similar = df[(df['cluster'] == cluster_id)]

    if not similar.empty:
        top_movies = similar.groupby('movie_title')['rating'] \
                            .mean().sort_values(ascending=False).head(top_n).index.tolist()
        return top_movies

    fallback = df['movie_title'].unique().tolist()
    random.shuffle(fallback)
    return fallback[:top_n]



# --------------------------------------------------------------------
# MOVIE DETAIL LOOKUP (FOR MOVIE PAGE)
# --------------------------------------------------------------------
def get_details(movie_title):
    conn = sqlite3.connect(DB_PATH)

    query = """
    SELECT title, genre, Director, Starring, Synopsis
    FROM Movies
    WHERE title = ?
    """

    try:
        details_df = pd.read_sql(query, conn, params=(movie_title,))
    except Exception as e:
        conn.close()
        return {"error": f"Query failed: {e}"}

    conn.close()

    if details_df.empty:
        return {"error": "No details found."}

    details = details_df.iloc[0].to_dict()

    # Convert numpy to normal Python values
    for k, v in details.items():
        if isinstance(v, (np.int64, np.float64)):
            details[k] = v.item()

    return details
