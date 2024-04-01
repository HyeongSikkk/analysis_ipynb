from dbConnect import con, cur, engine

import datetime
from multiprocessing import Pool
import requests
import pandas as pd
import re
import json

# 정규표현식 컴파일
pattern = re.compile(r'\{.*?\"_T\".*?\}')

def game_data(row) :
    try :
        asset_url = row['asset_url']
        match_id = row['match_id']
        try :
            req = requests.get(asset_url)
        except :
            cur.execute(f'INSERT INTO get_asset_error (match_id, asset_url) VALUES("{match_id}", "{asset_url}");')
        first = None
        if req.status_code != 200 :
            cur.execute(f'INSERT INTO get_asset_error (match_id, asset_url) VALUES("{match_id}", "{asset_url}");')

        is_json = True
        try :
            assets = req.json()
        except :
            is_json = False
            matches = pattern.findall(req.text)

        # 유저들 파악
        account_ids = set()
        # 유저들의 경로 파악
        positions = []
        record_bool = {}
        wanted_position = ['LogParachuteLanding', 
                            'LogPlayerPosition', 
                            'LogSwimStart', 
                            'LogSwimEnd', 
                            'LogVehicleRide', 
                            'LogVehicleLeave',
                            ]
        # 교전 관련 데이터 파악
        tds = []

        # 무기 관련 데이터 파싱
        kv2 = []

        # 부활 관련 데이터 파싱
        redeploys = []

        # 오브젝트 관련 데이터 파싱
        pc = []
        gsp = []
        air = []

        if is_json :
            for asset in assets :

                # 무기, 킬로그 데이터 관련 파싱
                if asset['_T'] == 'LogPlayerKillV2' :
                    assisters = {'assisters' : asset['assists_AccountId']}
                    if len(assisters['assisters']) == 0 :
                        assisters = None

                    victim_weapon = None if len(asset['victimWeapon']) == 0 else asset['victimWeapon']
                    if victim_weapon is not None :
                        tmp = victim_weapon.split('_C')
                        victim_weapon = ''.join(tmp[:-1])+'_C'

                    v2row = {
                        'match_id' : match_id,
                        'killer' : None if asset['killer'] is None != 40 else asset['killer']['accountId'],
                        'killer_weapon' : None if len(asset['killerDamageInfo']['damageCauserName']) == 0 else asset['killerDamageInfo']['damageCauserName'],
                        'killer_parts' : None if len(asset['killerDamageInfo']['additionalInfo']) == 0 else asset['killerDamageInfo']['additionalInfo'],
                        'killer_distance' :asset['killerDamageInfo']['distance'],
                        'victim' :asset['victim']['accountId'],
                        'victim_weapon' : victim_weapon,
                        'victim_parts' : None if len(asset["victimWeaponAdditionalInfo"]) == 0 else asset["victimWeaponAdditionalInfo"],
                        'assisters' : assisters,
                        'elapsed_time' : (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds
                    }
                    kv2.append(v2row)

                # 교전 데이터 관련 파싱
                elif asset['_T'] == 'LogPlayerTakeDamage' :
                    if not asset['attacker'] is None and not asset['attacker']['name'] == asset['victim']['name'] and asset['damage'] != 0:
                        td_row = {
                            'match_id' : match_id,
                            'attacker' : asset['attacker']['accountId'],
                            'attacker_x' : asset['attacker']['location']['x'],
                            'attacker_y' : asset['attacker']['location']['y'],
                            'victim' : asset['victim']['accountId'],
                            'victim_x' : asset['victim']['location']['x'],
                            'victim_y' : asset['victim']['location']['y'],
                            'use_weapon' : asset['damageCauserName'],
                            'damage_reason' : asset['damageReason'],
                            'damage' : asset['damage'],
                            'elapsed_time' : (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds,
                        }
                        tds.append(td_row)

                # 오브젝트 관련 파싱
                # 자기장 파악
                elif asset['_T'] == 'LogPhaseChange' :
                    ph_row = {
                        'phase' : asset['phase'],
                        '_D' : asset['_D']
                    }
                    pc.append(ph_row)
                # 자기장 파악
                elif asset['_T'] == 'LogGameStatePeriodic' :
                    small_row = {
                        'elapsedTime' : asset['gameState']['elapsedTime'],
                        'safety_zone_radius' : asset['gameState']['safetyZoneRadius'],
                        '_D' : asset['_D']
                    }
                    gsp_row = {**small_row, **asset['gameState']['safetyZonePosition']}
                    gsp.append(gsp_row)

                # 비행기 경로 파악
                elif asset['_T'] == 'LogVehicleLeave' :
                    if asset['vehicle']['vehicleType'] == 'TransportAircraft' :
                        vl_row = {'account_id' : asset['character']['accountId'],
                                '_D' : asset['_D']
                                }
                        air.append({**vl_row, **asset['character']['location']})


                elif asset['_T'] == "LogMatchStart" :
                        start_time = datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ')

                # 부활한 유저
                elif asset['_T'] == 'LogPlayerRedeployBRStart' :
                    for who in asset['characters'] :
                        redeploys.append(who['accountId'])

                # 이동경로 관련 파싱
                if 'character' in asset.keys() :        
                    account_id = asset['character']['accountId']
                    if account_id not in record_bool :
                        record_bool[account_id] = False

                    if asset['_T'] == 'LogParachuteLanding' :
                        record_bool[account_id] = True

                    if record_bool[account_id] :
                        if asset['_T'] in wanted_position :
                            created_at = asset['_D']
                            small_row = {
                                'match_id' : match_id,
                                'account_id' : asset['character']['accountId'],
                                'event' : asset['_T'],
                                'elapsed_time' : (datetime.datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds,
                                }
                            position_row = {**small_row, ** asset['character']['location']}
                            positions.append(position_row)

                    if 'ai' not in asset['character']['accountId']:
                        account_ids.add(asset['character']['accountId'])

        else :
            for match in matches :
                try :
                    asset = json.loads(match)
                except :
                    match = match.replace(':-nan', ':null')
                    match = match.replace(':nan', ':null')
                    asset = json.loads(match)    

                # 무기, 킬로그 데이터 관련 파싱
                if asset['_T'] == 'LogPlayerKillV2' :
                    assisters = {'assisters' : asset['assists_AccountId']}
                    if len(assisters['assisters']) == 0 :
                        assisters = None

                    victim_weapon = None if len(asset['victimWeapon']) == 0 else asset['victimWeapon']
                    if victim_weapon is not None :
                        tmp = victim_weapon.split('_C')
                        victim_weapon = '_C'.join(tmp[:-1])+'_C'

                    v2row = {
                        'match_id' : match_id,
                        'killer' : None if asset['killer'] is None != 40 else asset['killer']['accountId'],
                        'killer_weapon' : None if len(asset['killerDamageInfo']['damageCauserName']) == 0 else asset['killerDamageInfo']['damageCauserName'],
                        'killer_parts' : None if len(asset['killerDamageInfo']['additionalInfo']) == 0 else asset['killerDamageInfo']['additionalInfo'],
                        'killer_distance' :asset['killerDamageInfo']['distance'],
                        'victim' :asset['victim']['accountId'],
                        'victim_weapon' : victim_weapon,
                        'victim_parts' : None if len(asset["victimWeaponAdditionalInfo"]) == 0 else asset["victimWeaponAdditionalInfo"],
                        'assisters' : assisters,
                        'elapsed_time' : (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds
                    }
                    kv2.append(v2row)

                # 교전 데이터 관련 파싱
                elif asset['_T'] == 'LogPlayerTakeDamage' :
                    if not asset['attacker'] is None and not asset['attacker']['name'] == asset['victim']['name'] and asset['damage'] != 0:
                        td_row = {
                            'match_id' : match_id,
                            'attacker' : asset['attacker']['accountId'],
                            'attacker_x' : asset['attacker']['location']['x'],
                            'attacker_y' : asset['attacker']['location']['y'],
                            'victim' : asset['victim']['accountId'],
                            'victim_x' : asset['victim']['location']['x'],
                            'victim_y' : asset['victim']['location']['y'],
                            'use_weapon' : asset['damageCauserName'],
                            'damage_reason' : asset['damageReason'],
                            'damage' : asset['damage'],
                            'elapsed_time' : (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds,
                        }
                        tds.append(td_row)

                # 오브젝트 관련 파싱
                # 자기장 파악
                elif asset['_T'] == 'LogPhaseChange' :
                    ph_row = {
                        'phase' : asset['phase'],
                        '_D' : asset['_D']
                    }
                    pc.append(ph_row)
                # 자기장 파악
                elif asset['_T'] == 'LogGameStatePeriodic' :
                    small_row = {
                        'elapsedTime' : asset['gameState']['elapsedTime'],
                        'safety_zone_radius' : asset['gameState']['safetyZoneRadius'],
                        '_D' : asset['_D']
                    }
                    gsp_row = {**small_row, **asset['gameState']['safetyZonePosition']}
                    gsp.append(gsp_row)

                # 비행기 경로 파악
                elif asset['_T'] == 'LogVehicleLeave' :
                    if asset['vehicle']['vehicleType'] == 'TransportAircraft' :
                        vl_row = {'account_id' : asset['character']['accountId'],
                                '_D' : asset['_D']
                                }
                        air.append({**vl_row, **asset['character']['location']})


                elif asset['_T'] == "LogMatchStart" :
                        start_time = datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ')

                # 부활한 유저
                elif asset['_T'] == 'LogPlayerRedeployBRStart' :
                    for who in asset['characters'] :
                        redeploys.append(who['accountId'])

                # 이동경로 관련 파싱
                if 'character' in asset.keys() :        
                    account_id = asset['character']['accountId']
                    if account_id not in record_bool :
                        record_bool[account_id] = False

                    if asset['_T'] == 'LogParachuteLanding' :
                        record_bool[account_id] = True

                    if record_bool[account_id] :
                        if asset['_T'] in wanted_position :
                            created_at = asset['_D']
                            small_row = {
                                'match_id' : match_id,
                                'account_id' : asset['character']['accountId'],
                                'event' : asset['_T'],
                                'elapsed_time' : (datetime.datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds,
                                }
                            position_row = {**small_row, ** asset['character']['location']}
                            positions.append(position_row)

                    if 'ai' not in asset['character']['accountId']:
                        account_ids.add(asset['character']['accountId'])
        # end if else
            
        # 오브젝트 관련 데이터 만들기
        try :
            zone = pd.DataFrame(gsp)
            pcs = pd.DataFrame(pc)
            pcs = pcs.sort_values(by = '_D', ascending = False).drop_duplicates('phase').sort_values(by = '_D')
            zone['phase'] = 0
            for index, row in pcs.iterrows() :
                filter1 = zone['_D'] >= row['_D']
                zone.loc[filter1, 'phase'] = row['phase']
            zone_list = []
            for index, row in zone.sort_values(by = '_D', ascending = False).drop_duplicates('phase').sort_values(by = '_D').iterrows() :
                rows = {'phase' : row['phase'],
                        'x' : row['x'],
                        'y' : row['y'],
                        'z' : row['z'],
                        'radius' : row['safety_zone_radius'],
                        '_D' : row['_D']
                        }
                zone_list.append(rows)
        except :
            zone_list = None

        # 데이터 적재
        pd.DataFrame(positions).to_sql(name = 'position', con = engine, schema = 'pdgg', if_exists = 'append', index = False)
        pd.DataFrame(tds).to_sql(name = 'take_damage', con = engine, schema = 'pdgg', if_exists = 'append', index = False)

        kv2 = pd.DataFrame(kv2)
        kv2['assisters'] = kv2['assisters'].apply(lambda x : json.dumps(x))
        kv2['killer_parts'] = kv2['killer_parts'].apply(lambda x : str(x).replace('[', '').replace(']', ''))
        kv2['victim_parts'] = kv2['victim_parts'].apply(lambda x : str(x).replace('[', '').replace(']', ''))
        kv2.to_sql(name = 'killv2', con = engine, schema = 'pdgg', if_exists = 'append', index = False)

        try :
            firstX = air[0]['x']
            firstY = air[0]['y']
            lastX = air[len(air) // 2]['x']
            lastY = air[len(air) // 2]['y']
            airplane = {
                'firstX' : firstX,
                'firstY' : firstY,
                'lastX' : lastX,
                'lastY' : lastY,
            }
        except :
            airplane = None

        if len(redeploys) == 0 :
            redeploys = None
        data = [{'match_id' : match_id, 'airplane' : json.dumps(airplane), 'zone' : json.dumps(zone_list), 'redeploy' : json.dumps(redeploys)},]
        pd.DataFrame(data).to_sql(name = 'object', con = engine, schema = 'pdgg', if_exists = 'append', index = False)
        cur.execute(f'DELETE FROM get_asset WHERE match_id = "{match_id}";')
        con.commit()
        return account_ids
    except :
        cur.execute(f'''INSERT INTO get_asset_error (match_id, asset_url) VALUES ("{match_id}", "{asset_url}");''')
        con.commit()
        return None
    


def multiwork_get_game_data(dict_list) :
    pool = Pool()
    result = []
    result.append(pool.map(game_data, dict_list))
    pool.close()
    pool.join()
    return result[0]

cur.execute("SELECT * FROM get_asset;")
get_assets = cur.fetchall()
get_assets = list(map(lambda x : {'match_id' : x[0], 'asset_url' : x[1]}, get_assets))
r = multiwork_get_game_data(get_assets)
users =set()
for user_ids in r :
    if user_ids is None :
        continue
    for user in user_ids :
        users.add(user)

for user in users :
    try :
        cur.execute(f'''INSERT INTO target_users VALUES ("{user}")''')
    except :
        pass
con.commit()