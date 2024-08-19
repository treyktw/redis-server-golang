import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3

urllib3.disable_warnings()

REDIS_URL = "https://localhost:6379"
API_KEY = "your-secret-api-key"
BATCH_SIZE = 100

def redis_request(method, endpoint, data=None, params=None):
    headers = {"X-API-Key": API_KEY}
    url = f"{REDIS_URL}/{endpoint}"
    response = requests.request(method, url, headers=headers, json=data, params=params, verify=False)
    return response.json()

def load_json_file(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def cache_data(key_prefix, data):
    for item in data:
        key = f"{key_prefix}:{item.get('video_id', item.get('image_id', item.get('user_id')))}"
        redis_request("POST", "set", params={"key": key, "value": json.dumps(item)})

def redis_batch_set(batch):
    headers = {"X-API-Key": API_KEY}
    url = f"{REDIS_URL}/mset"
    response = requests.post(url, headers=headers, json=batch, verify=False)
    return response.json()

def cache_data_batch(data_type, data, start_index):
    batch = {}
    for i, item in enumerate(data[start_index:start_index+BATCH_SIZE]):
        key = f"{data_type}:{item.get('video_id', item.get('image_id', item.get('user_id')))}"
        batch[key] = json.dumps(item)
    return redis_batch_set(batch)

def benchmark_caching(data_type, filename):
    data = load_json_file(filename)
    print(f"Loaded {len(data)} {data_type} entries")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(0, len(data), BATCH_SIZE):
            futures.append(executor.submit(cache_data_batch, data_type, data, i))
        
        for future in as_completed(futures):
            future.result()  # This will raise an exception if the task failed
    
    end_time = time.time()
    
    print(f"Cached {len(data)} {data_type} entries in {end_time - start_time:.2f} seconds")

def main():
    while True:
        benchmark_caching("video", "video_analysis_benchmark.json")
        benchmark_caching("image", "image_metadata_benchmark.json")
        benchmark_caching("user", "user_data_benchmark.json")
        print("Waiting for 5 minutes before the next run...")
        time.sleep(300)  # Sleep for 5 minutes

if __name__ == "__main__":
    main()
