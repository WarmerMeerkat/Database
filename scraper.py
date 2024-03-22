import requests
import csv

# Function to fetch movie data from TMDB API
def fetch_movie_data(movie_id, api_key):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&append_to_response=credits,release_dates"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Function to fetch genre data from TMDB API
def fetch_genre_data(genre_id, api_key):
    url = f"https://api.themoviedb.org/3/genre/{genre_id}?api_key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()["name"]
    else:
        return None

# Function to extract age rating from release data
def extract_age_rating(movie_data):
    release_dates = movie_data.get("release_dates", {}).get("results", [])
    for release in release_dates:
        if release["iso_3166_1"] == "US":  # Assuming you want the US age rating
            certifications = release.get("release_dates", [])
            for certification in certifications:
                if certification["certification"]:
                    return certification["certification"]
    return None

# Sample usage
api_key = "a5ea79000e43568dddadc19e1033848c"
genre_id = 18  # Genre ID for Drama
output_file = "D:\movies.csv"

# Fetch genre name
genre_name = fetch_genre_data(genre_id, api_key)

if genre_name:
    # Write CSV header
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["Title", "ReleaseDate", "GenreName", "Director", "LeadActors", "SupportingActors", "Language", "Runtime", "Budget", "BoxOfficeRevenue", "Rating", "ReleaseType"])
        writer.writeheader()

    # Fetch movie data
    for page in range(1, 11):  # Fetching data from multiple pages (assuming 10 pages max)
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}&with_genres={genre_id}&page={page}"
        response = requests.get(url)
        if response.status_code == 200:
            movies = response.json()["results"]
            for movie in movies:
                movie_data = fetch_movie_data(movie["id"], api_key)
                if movie_data:
                    # Extract relevant information
                    title = movie_data["title"]
                    release_date = movie_data["release_date"]
                    director = next((crew["name"] for crew in movie_data["credits"]["crew"] if crew["job"] == "Director"), None)
                    lead_actors = ", ".join([cast["name"] for cast in movie_data["credits"]["cast"][:3]])  # Get top 3 lead actors
                    supporting_actors = ", ".join([cast["name"] for cast in movie_data["credits"]["cast"][3:6]])  # Get supporting actors
                    language = movie_data["original_language"]
                    runtime = movie_data["runtime"]
                    budget = movie_data["budget"]
                    box_office_revenue = movie_data["revenue"]
                    rating = movie_data["vote_average"]
                    release_type = extract_age_rating(movie_data)  # Fetch age rating

                    # Write data to CSV
                    with open(output_file, mode='a', newline='', encoding='utf-8') as file:
                        writer = csv.writer(file)
                        writer.writerow([title, release_date, genre_name, director, lead_actors, supporting_actors, language, runtime, budget, box_office_revenue, rating, release_type])
        else:
            print(f"Failed to fetch data for page {page}")

else:
    print("Failed to fetch genre data.")
