# 직접 만든 모듈
from authors import authors
from multi_tool import multi_tool
from get_funcs import get_users, get_match
from dbConnect import con, cur, engine

# 외부 패키지
import pandas as pd
import datetime
import json
import re
import tqdm
import time

while True :
    cur.execute(f"SELECT account_id FROM target_users ORDER BY RAND() LIMIT {len(authors) * 100};")
    target_users = cur.fetchall()
    idx = 0
    users_matches = []
    
    # api 교차사용
    num_api = 0
    # api 호출속도
    count_apis = len(authors)
    max_tries = count_apis * 10
    tries = 0
    cur_minute = datetime.datetime.today().minute
    start = time.time()
    while idx < len(target_users) :        
    
        # 제한 속도 걸렸을 경우 속도 제한
        if tries == max_tries :
            sleep_second = 60 - datetime.datetime.today().second
    
        # 매 분마다 속도제한 초기화
        if cur_minute != datetime.datetime.today().minute :
            cur_minute = datetime.datetime.today().minute
            tries = 0
    
        api = authors[num_api%count_apis]
        account_ids = ''
        for a in target_users[idx:idx+10] :
            account_ids += ','+a[0]
        result = get_users(account_ids[1:], api)
        if type(result) == list :
            users_matches += result
        idx += 10
        tries += 1
        num_api += 1
    matches = set(map(lambda x : x['id'], users_matches))
    pd.DataFrame(matches, columns = ['match_id']).to_sql(name = 'test_exist_match_id', con = engine, schema = 'pdgg', if_exists = 'append', index = False)
    cur.execute("SELECT sub.match_id,main.map_name FROM test_exist_match_id as sub LEFT OUTER JOIN match_summary as main ON sub.match_id = main.match_id WHERE map_name is NULL;")
    matches = cur.fetchall()
    cur.execute("DELETE FROM test_exist_match_id;")
    cur.fetchall()
    
    match_datas = []
    num_api = 0
    # api 호출속도
    count_apis = len(authors)
    max_tries = count_apis * 10
    tries = 0
    cur_minute = datetime.datetime.today().minute
    start = time.time()
    for match_id in tqdm.tqdm(matches) :
        # 제한 속도 걸렸을 경우 속도 제한
        if tries == max_tries :
            df = pd.DataFrame(match_datas)
            df.to_sql(name = 'match_summary', con = engine, schema = 'pdgg', if_exists = 'append', index = False)
            match_datas = []
            sleep_second = 60 - datetime.datetime.today().second
            time.sleep(sleep_second)
    
        # 매 분마다 속도제한 초기화
        if cur_minute != datetime.datetime.today().minute :
            cur_minute = datetime.datetime.today().minute
            tries = 0
    
        api = authors[num_api%count_apis]
        result = get_match(match_id[0], api)
        if type(result) == list :
            match_datas += result
        tries += 1
        num_api += 1
    df = pd.DataFrame(match_datas)
    df.to_sql(name = 'match_summary', con = engine, schema = 'pdgg', if_exists = 'append', index = False)