import requests

def get_users(account_ids, api) :
    header = {
        'Authorization': api,
        'Accept' : 'application/vnd.api+json'
    }
    url = f"https://api.pubg.com/shards/kakao/players?filter[playerIds]={account_ids}"
    try :
        request = requests.get(url, headers = header)
    except :
        return 'request error'
    
    # 오류 발생했을 경우의 예외처리
    if request.status_code != 200 :
        return 'status not 200 error'
    request = request.json()
    if 'errors' in request.keys() :
        return 'errors in request error'
    
    try :
        matches = []
        for man in request['data'] :
            matches += man['relationships']['matches']['data']
            
        return matches
    except :
        return 'has not matches error'
    
def get_match(match_id, author) :
    header = {
        'Authorization': author,
        'Accept' : 'application/vnd.api+json'
    }
    url = f"https://api.pubg.com/shards/kakao/matches/{match_id}"
    try :
        request = requests.get(url, headers = header)
    except :
        return 'request error'
    
    # 오류 발생했을 경우의 예외처리
    if request.status_code != 200 :
        return 'status not 200 error'
    
    request = request.json()
    if 'errors' in request.keys() :
        return 'errors in request error'
    
    try :
        row = {'match_id' : request['data']['id'],
               'map_name' : request['data']['attributes']['mapName'],
               'game_mode' : request['data']['attributes']['gameMode'],
               'match_type' : request['data']['attributes']['matchType'],
               'created_at' : request['data']['attributes']['createdAt'].replace('T', ' ').replace('Z', ''),
               'asset_url' : list(filter(lambda x : x['type'] == 'asset', request['included']))[0]['attributes']['URL']
              }
        return [row]
    except :
        return 'has not matches error'