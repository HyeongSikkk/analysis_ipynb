# -*- coding: utf-8 -*-
import datetime
import requests
import pandas as pd
import numpy as np
import time
import multi2 as am
from authors import authors # api 키들

# 사용자 정의 함수들
def get_user(account_id, api) :
    header = {
        'Authorization': api,
        'Accept' : 'application/vnd.api+json'
    }
    url = f"https://api.pubg.com/shards/kakao/players/{account_id}"
    try :
        request = requests.get(url, headers = header)
    except :
        return 'request error'
    
    # 오류 발생했을 경우의 예외처리
    if request.status_code != 200 :
        return 'status not 200 error'
    
    if 'errors' in request.json().keys() :
        return 'errors in request error'
    
    try :
        return request.json()['data']['relationships']['matches']['data']
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
               'created_at' : request['data']['attributes']['createdAt'],
               'shard_id' : request['data']['attributes']['shardId'],
               'asset_url' : list(filter(lambda x : x['type'] == 'asset', request['included']))[0]['attributes']['URL']
              }
        return [row]
    except :
        return 'has not matches error'

apis = authors
while True :
    # api 호출속도
    count_apis = len(apis)
    max_tries = count_apis * 10
    tries = 0
    cur_minute = datetime.datetime.today().minute

    # api 교차사용
    num_api = 0

    # 유저 정보 호출 후 랜덤으로 유저 100명 요청
    users = pd.read_parquet("parquets/target_users.parquet")
    target_size = 50 # test
    users_list = users.index.to_numpy()
    np.random.shuffle(users_list)

    users_matches = []
    for index, row in users.iloc[users_list].iterrows() :

        # 원하는 갯수 달성 시 루프 종료
        if target_size == 0 :
            break

        # 제한 속도 걸렸을 경우 속도 제한
        if tries == max_tries :
            sleep_second = 60 - datetime.datetime.today().second
            time.sleep(sleep_second)

        # 매 분마다 속도제한 초기화
        if cur_minute != datetime.datetime.today().minute :
            cur_minute = datetime.datetime.today().minute
            tries = 0

        api = apis[num_api%count_apis]
        account_id = row['account_id']
        result = get_user(account_id, api)
        if type(result) == list :
            users_matches += result
            target_size -= 1

        tries += 1
        num_api += 1

    # 수집한 유저의 매치들 중에서 없는 경기들만 추출
    user_match_df = pd.DataFrame(users_matches)
    user_match_df = user_match_df.drop(columns = ['type'])
    user_match_df = user_match_df.drop_duplicates('id')
    user_match_df = user_match_df.rename(columns = {'id' : 'match_id'})

    match_summary = pd.read_parquet('parquets/match_summary.parquet')
    df = pd.merge(user_match_df, match_summary, how = 'left', on = 'match_id')
    match_target = df[df['map_name'].isnull()][['match_id']].copy()
    del user_match_df
    del df

    rows = []
    for index, row in match_target.iterrows() :
        # 제한 속도 걸렸을 경우 속도 제한
        if tries == max_tries :
            sleep_second = 60 - datetime.datetime.today().second
            time.sleep(sleep_second)

        # 매 분마다 속도제한 초기화
        if cur_minute != datetime.datetime.today().minute :
            cur_minute = datetime.datetime.today().minute
            tries = 0

        api = apis[num_api%count_apis]
        match_id = row['match_id']
        result = get_match(match_id, api)
        if type(result) == list :
            rows += result

        tries += 1
        num_api += 1
    try :
        new_match_summary = pd.DataFrame(rows)
        nms = new_match_summary[['match_id', 'map_name', 'game_mode', 'created_at', 'shard_id']]
        pd.concat([match_summary, nms], axis = 0).reset_index(drop = True).to_parquet('parquets/match_summary.parquet')
    
    except :
        continue

    get_log_list = []
    for index, row in new_match_summary.iterrows() :
        rows = {'match_id' : row['match_id'],
                'asset_url' : row['asset_url'],
                }
        get_log_list.append(rows)
    # Pool을 이용한 병렬처리
    am_result = am.multiwork_get_game_data(get_log_list)
    # 멀티프로세싱 결과값으로부터 유저 아이디들 분리
    account_ids = []
    for r in am_result[0] :
        r_account_ids = r.popitem()
        if r_account_ids[1] != None :
            account_ids += r_account_ids[1]

    # 멀티프로세싱 결과값들 기존 자료와 결합하여 저장
    new_object = pd.DataFrame(am_result[0])
    old_object = pd.read_parquet('parquets/object.parquet')
    pd.concat([old_object, new_object], axis = 0).reset_index(drop = True).to_parquet('parquets/object.parquet')

    # 유저 아이디들 합친 후 중복값 제거하여 저장
    new_account = pd.DataFrame({'account_id' : account_ids})
    accounts = pd.concat([users, new_account], axis = 0)
    accounts = accounts.drop_duplicates('account_id')
    accounts.reset_index(drop = True).to_parquet("parquets/target_users.parquet", engine = 'pyarrow')