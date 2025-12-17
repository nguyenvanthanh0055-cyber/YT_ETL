from dotenv import load_dotenv
import requests
import os
import json
from datetime import date
load_dotenv()

API_KEY = os.getenv('API_KEY')
CHANNEL_HANDLE = 'MrBeast'
max_result = 50

def get_playlist_id():
    try:
        base_url = 'https://youtube.googleapis.com/youtube/v3/channels'
        params = {
          "part": "contentDetails",
          "forHandle": CHANNEL_HANDLE,
          "key": API_KEY  
        }
        response = requests.get(base_url,params=params)
        data = response.json()
        # print(json.dumps(data,indent=4))
        channel_items = data['items']
        channel_playlistId = channel_items[0]['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlistId
    except requests.exceptions.RequestException as e: 
        raise e
    
    
def get_video_ids(playlist_id):
    
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems"
    params = {
        'part': 'contentDetails',
        'maxResults': max_result,
        'playlistId': playlist_id,
        'key': API_KEY
    }
    
    video_ids = []
    page_token = None
    try:
        while True:
            
            if page_token:
                params['pageToken'] = page_token
            else:
                params.pop("pageToken", None)
            
            response = requests.get(base_url,params=params)
            response.raise_for_status()
            
            data = response.json()
            # print(json.dumps(data,indent=5))
            for item in data.get('items',[]):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
        
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        return video_ids
    except requests.exceptions.RequestException as e: 
        raise e
    

def extract_video_data(video_ids):
    extract_data = []
    
    def batch_list(video_id_lst, batch_size):
        for video_id in range(0,len(video_id_lst),batch_size):
            yield video_id_lst[video_id:video_id+batch_size]
    
    url = "https://youtube.googleapis.com/youtube/v3/videos" 
    
    try:
        for batch in batch_list(video_ids,max_result):
            video_ids_str = ",".join(batch)

            params = {
                'part': 'contentDetails,snippet,statistics',
                'id': video_ids_str,
                'key': API_KEY     
            } 
        
        
            response = requests.get(url,params)
            response.raise_for_status()
            
            data = response.json()
            
            for item in data.get('items',[]):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
                
                
                extract_data.append({
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    "viewCount": statistics.get('viewCount',None),
                    "likeCount": statistics.get('likeCount',None),
                    "commentCount": statistics.get('commentCount',None)
                })

        return extract_data
    except requests.exceptions.RequestException as e:
        raise e
    
def save_to_json(extract_data):
    file_path = f"./data/{date.today()}.json"
    with open(file_path,'w',encoding='utf-8') as json_outfile:
        json.dump(extract_data, json_outfile, indent=4, ensure_ascii=False)
        
if __name__ == '__main__':
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
    