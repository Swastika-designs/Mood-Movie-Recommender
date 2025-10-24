from flask import Flask, request, jsonify, render_template
from recommendation import recommend_movies

app = Flask(__name__, template_folder='../frontend')

# Recommendation endpoint
@app.route('/recommend', methods=['GET'])
def recommend():
    mood = request.args.get('mood')
    activity = request.args.get('activity')
    if not mood or not activity:
        return jsonify({'error': 'Mood and activity are required'}), 400
    movies = recommend_movies(mood, activity, top_n=5)
    return jsonify({'recommended_movies': movies})

# Feedback endpoint (future use)
@app.route('/feedback', methods=['POST'])
def feedback():
    data = request.get_json()
    movie = data.get('movie')
    fb = data.get('feedback')  # 'like' or 'dislike'
    print(f"User gave {fb} to {movie}")
    # Future: store feedback in the database for learning
    return jsonify({'message': f'{fb.capitalize()} recorded for {movie}'})

# Frontend page
@app.route('/')
def home():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
