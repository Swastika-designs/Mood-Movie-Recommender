from flask import Flask, request, jsonify, render_template, abort
from recommendation import recommend_movies, get_details # Import necessary functions
import sqlite3 # Import sqlite3 for use in the app context

# Define database path globally
DB_PATH = 'database/warehouse.db'

# Configure Flask to look for templates (HTML files) in the '../frontend' directory 
app = Flask(__name__, template_folder='../frontend')

# --- FRONTEND ROUTING (Serving HTML pages) ---

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/<filename>.html')
def serve_html(filename):
    try:
        return render_template(filename + '.html')
    except Exception as e:
        print(f"Error serving {filename}.html: {e}")
        abort(404)


# --- API ENDPOINTS ---

# 1. Recommendation endpoint (/recommend)
@app.route('/recommend', methods=['GET'])
def recommend():
    mood = request.args.get('mood')
    activity = request.args.get('activity')
    top_n = request.args.get('top_n', 5, type=int) 
    
    if not mood or not activity:
        return jsonify({'error': 'Mood and activity are required'}), 400
    
    # Pass DB_PATH to the recommendation function
    movies = recommend_movies(mood, activity, top_n=top_n)
    
    return jsonify({'recommended_movies': movies})

# 2. DETAIL LOOKUP endpoint (/detail)
@app.route('/detail', methods=['GET'])
def detail():
    title = request.args.get('title')
    if not title:
        return jsonify({'error': 'Movie title is required for details lookup.'}), 400
    
    # Pass DB_PATH to the detail lookup function
    details = get_details(title)
    
    if 'error' in details:
        # Passes through errors from the recommendation module (e.g., "Movie not found")
        return jsonify(details), 404
    
    return jsonify(details)


# 3. Feedback endpoint
@app.route('/feedback', methods=['POST'])
def feedback():
    data = request.get_json()
    movie = data.get('movie')
    fb = data.get('feedback')
    print(f"User gave {fb} to {movie}")
    return jsonify({'message': f'{fb.capitalize()} recorded for {movie}'})


if __name__ == '__main__':
    # Running on port 5000
    app.run(debug=True, port=5000)