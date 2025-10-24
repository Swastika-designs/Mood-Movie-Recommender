-- ===============================================
-- Database Schema: Mood-Based Movie Recommender (Modernized)
-- ===============================================

-- 1️⃣ Users Table
CREATE TABLE IF NOT EXISTS Users (
    user_id INTEGER PRIMARY KEY,
    age INTEGER,
    gender TEXT,
    occupation TEXT
);

-- 2️⃣ Movies Table (modernized with optional genre/year)
CREATE TABLE IF NOT EXISTS Movies (
    movie_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    genre TEXT DEFAULT NULL,
    release_year INTEGER DEFAULT NULL
);

-- 3️⃣ Mood Table
CREATE TABLE IF NOT EXISTS Mood (
    mood_id INTEGER PRIMARY KEY,
    mood TEXT UNIQUE NOT NULL
);

-- 4️⃣ Activity Table
CREATE TABLE IF NOT EXISTS Activity (
    activity_id INTEGER PRIMARY KEY,
    activity TEXT UNIQUE NOT NULL
);

-- 5️⃣ Time Dimension Table
CREATE TABLE IF NOT EXISTS Time (
    time_id INTEGER PRIMARY KEY,
    hour INTEGER NOT NULL,
    day_of_week TEXT NOT NULL
);

-- 6️⃣ Fact Table: User Interactions
CREATE TABLE IF NOT EXISTS UserMovieInteraction (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating INTEGER NOT NULL,
    mood_id INTEGER NOT NULL,
    activity_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    FOREIGN KEY(user_id) REFERENCES Users(user_id),
    FOREIGN KEY(movie_id) REFERENCES Movies(movie_id),
    FOREIGN KEY(mood_id) REFERENCES Mood(mood_id),
    FOREIGN KEY(activity_id) REFERENCES Activity(activity_id),
    FOREIGN KEY(time_id) REFERENCES Time(time_id)
);
