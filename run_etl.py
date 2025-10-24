### ===============================================
# ETL Script: Hybrid MovieLens + TMDB Dataset
# Generates movie_data_clean.csv (Modernized)
# ===============================================

import pandas as pd
import numpy as np
import requests
import time

# 1️⃣ Load Original MovieLens 100K Dataset
ratings = pd.read_csv('data/ml-100k/u.data', sep='\t', names=['user_id', 'movie_id', 'rating', 'timestamp'])
movies = pd.read_csv('data/ml-100k/u.item', sep='|', encoding='latin-1', usecols=[0, 1], names=['movie_id', 'title'])
users = pd.read_csv('data/ml-100k/u.user', sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip'])

# 2️⃣ Merge Datasets
df = ratings.merge(movies, on='movie_id').merge(users, on='user_id')

# 3️⃣ Add Random Mood & Activity Columns
moods = ['Happy', 'Sad', 'Relaxed', 'Energetic']
activities = ['Studying', 'Relaxing', 'Working Out', 'Partying']
np.random.seed(42)
df['mood'] = np.random.choice(moods, len(df))
df['activity'] = np.random.choice(activities, len(df))

# 4️⃣ Convert Timestamp to Datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
df['time_of_day'] = df['timestamp'].dt.hour
df['day_of_week'] = df['timestamp'].dt.day_name()

# ----------------------------------------------------------
# 🆕 5️⃣ Fetch New Movie Titles from TMDB for Modern Variety
# ----------------------------------------------------------
print("🎬 Fetching latest popular movies from TMDB...")

tmdb_api = "https://api.themoviedb.org/3/movie/popular"
params = {"api_key": "b9bd48a43a56b37b8d33b184a3fdf5f4", "language": "en-US", "page": 1}
tmdb_movies = []

for p in range(1, 3):  # get top ~40 modern movies
    params["page"] = p
    response = requests.get(tmdb_api, params=params)
    if response.status_code == 200:
        results = response.json().get("results", [])
        for m in results:
            tmdb_movies.append({
                "movie_id": 100000 + m["id"],
                "title": m["title"],
                "rating": np.random.randint(3, 6),
                "user_id": np.random.randint(1, 944),
                "mood": np.random.choice(moods),
                "activity": np.random.choice(activities),
                "time_of_day": np.random.randint(0, 24),
                "day_of_week": np.random.choice(['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])
            })
    time.sleep(1)  # avoid hitting API limits

tmdb_df = pd.DataFrame(tmdb_movies)
print(f"✅ Added {len(tmdb_df)} new modern movies.")

# ----------------------------------------------------------
# 6️⃣ Combine with MovieLens Data (only if TMDB has data)
# ----------------------------------------------------------
if not tmdb_df.empty:
    df_modern = pd.concat([
        df[['user_id', 'movie_id', 'title', 'rating', 'mood', 'activity', 'time_of_day', 'day_of_week']],
        tmdb_df[['user_id', 'movie_id', 'title', 'rating', 'mood', 'activity', 'time_of_day', 'day_of_week']]
    ])
else:
    df_modern = df[['user_id', 'movie_id', 'title', 'rating', 'mood', 'activity', 'time_of_day', 'day_of_week']].copy()

# 7️⃣ Save the Cleaned Dataset
df_modern.to_csv('data/movie_data_clean.csv', index=False)
print("✅ Final modernized dataset saved as data/movie_data_clean.csv")
