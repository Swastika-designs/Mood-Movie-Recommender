import sqlite3
import pandas as pd
import os

# --- Paths and Setup ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_folder = os.path.join(BASE_DIR, 'database')
os.makedirs(db_folder, exist_ok=True)

db_path = os.path.join(db_folder, 'warehouse.db')
csv_path = os.path.join(BASE_DIR, '..', 'data', 'full_movie.csv')  # Corrected CSV path
schema_path = os.path.join(db_folder, 'schema.sql')

# --- Connect to Database ---
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 1️⃣ Drop existing tables (if any)
tables = ['UserMovieInteraction', 'Time', 'Activity', 'Mood', 'Movies', 'Users']
for t in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {t}")

# 2️⃣ Load schema.sql
try:
    with open(schema_path, 'r', encoding='utf-8') as f:
        cursor.executescript(f.read())
    conn.commit()
except FileNotFoundError:
    print(f"❌ ERROR: schema.sql not found at {schema_path}")
    conn.close()
    exit()

# 3️⃣ Load CSV safely
try:
    df = pd.read_csv(csv_path, on_bad_lines='skip', encoding='utf-8')
except FileNotFoundError:
    print(f"❌ ERROR: CSV not found at {csv_path}")
    conn.close()
    exit()

# --- Clean & Prepare Data ---
df.columns = df.columns.str.strip()  # Fix spaces like "    user_id"
print("📊 Columns detected:", list(df.columns))

# Fill missing user info if needed
for col in ['age', 'gender', 'occupation']:
    if col not in df.columns:
        df[col] = 'Unknown'

# --- Insert into Dimension Tables ---

# 4️⃣ Mood Table
if 'mood' in df.columns:
    moods = df['mood'].dropna().unique()
    for i, m in enumerate(moods):
        cursor.execute("INSERT OR IGNORE INTO Mood (mood_id, mood) VALUES (?, ?)", (i + 1, m))
else:
    moods = []

# 5️⃣ Activity Table
if 'activity' in df.columns:
    activities = df['activity'].dropna().unique()
    for i, a in enumerate(activities):
        cursor.execute("INSERT OR IGNORE INTO Activity (activity_id, activity) VALUES (?, ?)", (i + 1, a))
else:
    activities = []

# 6️⃣ Users Table
if 'user_id' in df.columns:
    for _, row in df[['user_id', 'age', 'gender', 'occupation']].drop_duplicates().iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO Users (user_id, age, gender, occupation)
            VALUES (?, ?, ?, ?)
        """, (row.user_id, row.age, row.gender, row.occupation))
else:
    print("⚠️ Skipping Users insertion — 'user_id' column missing")

# 7️⃣ Movies Table
if 'genres' in df.columns:
    df = df.rename(columns={'genres': 'genre'})

movie_columns = ['movie_id', 'title', 'genre', 'release_year', 'Director', 'Starring', 'Synopsis']
for col in movie_columns:
    if col not in df.columns:
        df[col] = None  # Fill missing columns to avoid key errors

movie_df_insert = df[movie_columns].drop_duplicates(subset=['movie_id'])
cols = ', '.join(movie_columns)
placeholders = ', '.join(['?'] * len(movie_columns))

for _, row in movie_df_insert.iterrows():
    values = tuple(row.get(col) for col in movie_columns)
    cursor.execute(f"INSERT OR IGNORE INTO Movies ({cols}) VALUES ({placeholders})", values)

# 8️⃣ Time Table
if 'time_of_day' in df.columns and 'day_of_week' in df.columns:
    times = df[['time_of_day', 'day_of_week']].drop_duplicates().reset_index(drop=True)
    for i, row in times.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO Time (time_id, hour, day_of_week)
            VALUES (?, ?, ?)
        """, (i + 1, row.time_of_day, row.day_of_week))
else:
    times = pd.DataFrame(columns=['time_of_day', 'day_of_week'])

# 9️⃣ ID Mappings
mood_map = {m: i + 1 for i, m in enumerate(moods)}
activity_map = {a: i + 1 for i, a in enumerate(activities)}
time_map = {(row.time_of_day, row.day_of_week): i + 1 for i, row in times.iterrows()}

# 🔟 UserMovieInteraction Fact Table
required_cols = ['user_id', 'movie_id', 'rating', 'mood', 'activity', 'time_of_day', 'day_of_week']
missing_cols = [c for c in required_cols if c not in df.columns]
if not missing_cols:
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
else:
    print(f"⚠️ Skipping UserMovieInteraction insertion — missing columns: {missing_cols}")

# ✅ Done
conn.commit()
conn.close()
print(f"✅ Data successfully loaded into {db_path}")
