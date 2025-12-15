from dotenv import load_dotenv
import requests
import os
import json
load_dotenv()

API_KEY = os.getenv('API_KEY')
CHANNEL_HANDLE = 'MrBeast'

def get_playlist_id():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}'
        response = requests.get(url)
        data = response.json()
        # print(json.dumps(data,indent=4))
        channel_items = data['items']
        channel_playlistId = channel_items[0]['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlistId
    except requests.exceptions.RequestException as e: 
        raise e
    
if __name__ == '__main__':
    print(get_playlist_id())