import sqlite3
import pandas as pd
import os

# 1️⃣ Ensure database folder exists
db_folder = 'database'
if not os.path.exists(db_folder):
    os.makedirs(db_folder)

# 2️⃣ Connect to SQLite database
db_path = os.path.join(db_folder, 'warehouse.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 3️⃣ Drop existing tables (if any)
tables = ['UserMovieInteraction', 'Time', 'Activity', 'Mood', 'Movies', 'Users']
for t in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {t}")

# 4️⃣ Load schema
schema_path = os.path.join(db_folder, 'schema.sql')
with open(schema_path, 'r', encoding='utf-8') as f:
    cursor.executescript(f.read())
conn.commit()

# 5️⃣ Load cleaned CSV
csv_path = os.path.join('..', 'data', 'movie_data_clean.csv')
df = pd.read_csv(csv_path)

# Fill missing user info for TMDB entries
for col in ['age', 'gender', 'occupation']:
    if col not in df.columns:
        df[col] = 'Unknown'

# 6️⃣ Insert into Mood table
moods = df['mood'].dropna().unique()
for i, m in enumerate(moods):
    cursor.execute("INSERT OR IGNORE INTO Mood (mood_id, mood) VALUES (?, ?)", (i+1, m))

# 7️⃣ Insert into Activity table
activities = df['activity'].dropna().unique()
for i, a in enumerate(activities):
    cursor.execute("INSERT OR IGNORE INTO Activity (activity_id, activity) VALUES (?, ?)", (i+1, a))

# 8️⃣ Insert into Users table
for _, row in df[['user_id', 'age', 'gender', 'occupation']].drop_duplicates().iterrows():
    cursor.execute("""
        INSERT OR IGNORE INTO Users (user_id, age, gender, occupation)
        VALUES (?, ?, ?, ?)
    """, (row.user_id, row.age, row.gender, row.occupation))

# 9️⃣ Insert into Movies table (handle optional genre/year)
movie_cols = [c for c in ['movie_id', 'title', 'genre', 'release_year'] if c in df.columns]
for _, row in df[movie_cols].drop_duplicates().iterrows():
    cursor.execute("""
        INSERT OR IGNORE INTO Movies (movie_id, title, genre, release_year)
        VALUES (?, ?, ?, ?)
    """, (
        row.get('movie_id'),
        row.get('title'),
        row.get('genre', None),
        row.get('release_year', None)
    ))

# 🔟 Insert into Time table
times = df[['time_of_day', 'day_of_week']].drop_duplicates().reset_index(drop=True)
for i, row in times.iterrows():
    cursor.execute("""
        INSERT OR IGNORE INTO Time (time_id, hour, day_of_week)
        VALUES (?, ?, ?)
    """, (i+1, row.time_of_day, row.day_of_week))

# 1️⃣1️⃣ Mapping
mood_map = {m: i+1 for i, m in enumerate(moods)}
activity_map = {a: i+1 for i, a in enumerate(activities)}
time_map = {(row.time_of_day, row.day_of_week): i+1 for i, row in times.iterrows()}

# 1️⃣2️⃣ Insert into Fact table
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO UserMovieInteraction (user_id, movie_id, rating, mood_id, activity_id, time_id)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        row.user_id,
        row.movie_id,
        row.rating if not pd.isna(row.rating) else 3,
        mood_map.get(row.mood),
        activity_map.get(row.activity),
        time_map.get((row.time_of_day, row.day_of_week))
    ))

# 1️⃣3️⃣ Commit and Close
conn.commit()
conn.close()
print(f"✅ Data successfully loaded into {db_path}")
