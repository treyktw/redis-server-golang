import json
import random
import string
from datetime import datetime, timedelta
import time

# Adjustable parameters
MIN_ENTRIES = 2000
MAX_ENTRIES = 2000 # Set to the same as MIN_ENTRIES for a fixed number
FRAMES_PER_VIDEO_MIN = 500
FRAMES_PER_VIDEO_MAX =  2500
VIDEO_DURATION_MIN = 1200  # in seconds
VIDEO_DURATION_MAX = 12000  # 5 minutes

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_date(start_date, end_date):
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    return start_date + timedelta(days=random_days)

def generate_video_analysis_data(num_entries):
    video_analysis = []
    for i in range(num_entries):
        video = {
            "video_id": f"vid_{generate_random_string(10)}",
            "title": f"Video {i+1}",
            "duration": random.randint(VIDEO_DURATION_MIN, VIDEO_DURATION_MAX),
            "upload_date": generate_random_date(datetime(2020, 1, 1), datetime.now()).isoformat(),
            "views": random.randint(100, 1000000),
            "likes": random.randint(10, 100000),
            "dislikes": random.randint(0, 10000),
            "frame_analysis": []
        }
        num_frames = random.randint(FRAMES_PER_VIDEO_MIN, FRAMES_PER_VIDEO_MAX)
        for j in range(num_frames):
            frame = {
                "timestamp": j * (video["duration"] / num_frames),
                "objects_detected": random.sample(["person", "car", "tree", "building", "animal", "sign", "road", "sky"], k=random.randint(1, 3)),
                "dominant_color": f"#{generate_random_string(6)}",
                "brightness": round(random.uniform(0, 1), 2),
                "motion_score": round(random.uniform(0, 1), 2)
            }
            video["frame_analysis"].append(frame)
        video_analysis.append(video)
    return video_analysis

def generate_image_metadata(num_entries):
    image_metadata = []
    for i in range(num_entries):
        image = {
            "image_id": f"img_{generate_random_string(10)}",
            "filename": f"image_{i+1}.jpg",
            "upload_date": generate_random_date(datetime(2020, 1, 1), datetime.now()).isoformat(),
            "size_kb": random.randint(100, 10000),
            "dimensions": {
                "width": random.choice([1920, 2560, 3840, 4096]),
                "height": random.choice([1080, 1440, 2160, 2304])
            },
            "format": random.choice(["JPEG", "PNG", "GIF", "WEBP"]),
            "color_space": random.choice(["sRGB", "Adobe RGB", "ProPhoto RGB"]),
            "has_alpha_channel": random.choice([True, False]),
            "dpi": random.choice([72, 96, 150, 300]),
            "camera_info": {
                "make": random.choice(["Canon", "Nikon", "Sony", "Fujifilm"]),
                "model": f"Camera Model {generate_random_string(5)}",
                "focal_length": f"{random.randint(10, 200)}mm",
                "aperture": f"f/{random.choice([1.4, 1.8, 2.0, 2.8, 4.0, 5.6])}"
            },
            "location": {
                "latitude": round(random.uniform(-90, 90), 6),
                "longitude": round(random.uniform(-180, 180), 6)
            },
            "tags": random.sample(["nature", "portrait", "landscape", "urban", "wildlife", "macro", "black and white"], k=random.randint(1, 3))
        }
        image_metadata.append(image)
    return image_metadata

def generate_user_data(num_entries):
    user_data = []
    for i in range(num_entries):
        user = {
            "user_id": f"user_{generate_random_string(10)}",
            "username": f"user_{generate_random_string(8)}",
            "email": f"{generate_random_string(8)}@example.com",
            "first_name": generate_random_string(6).capitalize(),
            "last_name": generate_random_string(8).capitalize(),
            "date_of_birth": generate_random_date(datetime(1950, 1, 1), datetime(2005, 12, 31)).isoformat(),
            "registration_date": generate_random_date(datetime(2010, 1, 1), datetime.now()).isoformat(),
            "last_login": generate_random_date(datetime(2022, 1, 1), datetime.now()).isoformat(),
            "account_status": random.choice(["active", "inactive", "suspended"]),
            "profile": {
                "bio": f"This is the bio for user {i+1}. " + generate_random_string(20),
                "website": f"https://www.{generate_random_string(10)}.com",
                "location": f"{generate_random_string(8)}, {generate_random_string(8)}",
                "interests": random.sample(["photography", "travel", "cooking", "sports", "music", "movies", "technology", "art"], k=random.randint(2, 4))
            },
            "social_media": {
                "twitter": f"@{generate_random_string(10)}",
                "instagram": f"@{generate_random_string(12)}",
                "facebook": f"fb.com/{generate_random_string(15)}"
            },
            "preferences": {
                "theme": random.choice(["light", "dark", "auto"]),
                "language": random.choice(["en", "es", "fr", "de", "ja"]),
                "notifications": random.choice([True, False])
            },
            "stats": {
                "posts": random.randint(0, 1000),
                "followers": random.randint(0, 100000),
                "following": random.randint(0, 1000)
            }
        }
        user_data.append(user)
    return user_data

def save_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def generate_and_serve_data():
    num_entries = random.randint(MIN_ENTRIES, MAX_ENTRIES)
    
    print(f"Generating video analysis data with {num_entries} entries...")
    video_analysis_data = generate_video_analysis_data(num_entries)
    save_json(video_analysis_data, "video_analysis_benchmark.json")
    print("Video analysis data saved to video_analysis_benchmark.json")

    print(f"Generating image metadata with {num_entries} entries...")
    image_metadata = generate_image_metadata(num_entries)
    save_json(image_metadata, "image_metadata_benchmark.json")
    print("Image metadata saved to image_metadata_benchmark.json")

    print(f"Generating user data with {num_entries} entries...")
    user_data = generate_user_data(num_entries)
    save_json(user_data, "user_data_benchmark.json")
    print("User data saved to user_data_benchmark.json")

    # Serve the data to the Redis cache
    main()  # This is where your caching function will be called

def main():
    while True:
        generate_and_serve_data()
        print("Sleeping for 5 minutes...")
        time.sleep(300)  # Sleep for 5 minutes

if __name__ == "__main__":
    main()