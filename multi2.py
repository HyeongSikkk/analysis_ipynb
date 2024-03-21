# -*- coding: utf-8 -*-
import datetime
from multiprocessing import Pool
import requests
import pandas as pd

def game_data(row) :
    asset_url = row['asset_url']
    match_id = row['match_id']
    try :
        ar = requests.get(asset_url)
    except :
        return {'match_id' : match_id, 'zone' : None, 'airplane' : None, 'first' : None, 'account_ids' : None}
    first = None
    if ar.status_code == 200 :
        # 데이터 파싱
        
        # 유저들 파악
        account_ids = []
        # 유저들의 경로 파악
        positions = []
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
        
        # 오브젝트 관련 데이터 파싱
        pc = []
        gsp = []
        air = []
        
        for log in ar.json() :
            
            # 무기 관련 데이터 파싱
            if log['_T'] == 'LogPlayerKillV2' :
                try :
                    if 'ai' not in log['victim']['accountId'] and 'ai' not in log['killer']['accountId'] and 'ai' not in log['finisher']['accountId'] :
                        try :
                            v2row = {'victim_weapon' : None if len(log['victimWeapon']) == 0 else log['victimWeapon'],
                                    'victim_account_id' :log['victim']['accountId'],
                                    'victim_parts' : None if len(log["victimWeaponAdditionalInfo"]) == 0 else log["victimWeaponAdditionalInfo"],
                                    'killer_weapon' : log['killerDamageInfo']['damageCauserName'],
                                    'killer_account_id' : log['killer']['accountId'],
                                    'killer_parts' : None if len(log['killerDamageInfo']['additionalInfo']) == 0 else log['killerDamageInfo']['additionalInfo'],
                                    'killer_distance' :log['killerDamageInfo']['distance'],
                                    'finisher_weapon' : log['finishDamageInfo']['damageCauserName'],
                                    'finisher_account_id' : log['finisher']['accountId'],
                                    'finisher_parts' : None if len(log['finishDamageInfo']['additionalInfo']) == 0 else log['finishDamageInfo']['additionalInfo'],
                                    'finisher_distance' : log['finishDamageInfo']['distance'],
                                    }
                        except :
                            v2row = {'victim_weapon' : None,
                                    'victim_account_id' : None,
                                    'victim_parts' : None,
                                    'killer_weapon' : None,
                                    'killer_account_id' : None,
                                    'killer_parts' : None,
                                    'killer_distance' : None,
                                    'finisher_weapon' : None,
                                    'finisher_account_id' : None,
                                    'finisher_parts' : None,
                                    'finisher_distance' : None,
                                    }
                    kv2.append(v2row)
                except :
                    pass
                
            # 교전 데이터 관련 파싱
            elif log['_T'] == 'LogPlayerTakeDamage' :
                if not log['attacker'] is None and not log['attacker']['name'] == log['victim']['name'] and log['damage'] != 0:
                    try :
                        td_row = {
                            'attacker' : log['attacker']['accountId'],
                            'attacker_x' : log['attacker']['location']['x'],
                            'attacker_y' : log['attacker']['location']['y'],
                            'victim' : log['victim']['accountId'],
                            'victim_x' : log['victim']['location']['x'],
                            'victim_y' : log['victim']['location']['y'],
                            'use_weapon' : log['damageCauserName'],
                            'damage_reason' : log['damageReason'],
                            'damage' : log['damage'],
                            'elapsed_time' : (datetime.datetime.strptime(log['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds,
                        }
                        tds.append(td_row)
                    except :
                        pass
            
            # 오브젝트 관련 파싱
            elif log['_T'] == 'LogPhaseChange' :
                ph_row = {'phase' : log['phase'],
                        '_D' : log['_D']}
                pc.append(ph_row)
    
            elif log['_T'] == 'LogGameStatePeriodic' :
                small_row = {'elapsedTime' : log['gameState']['elapsedTime'],
                        'safety_zone_radius' : log['gameState']['safetyZoneRadius'],
                        '_D' : log['_D']
                        }
                gsp_row = {**small_row, **log['gameState']['safetyZonePosition']}
                gsp.append(gsp_row)
                
            elif log['_T'] == 'LogVehicleLeave' :
                if log['vehicle']['vehicleType'] == 'TransportAircraft' :
                    vl_row = {'account_id' : log['character']['accountId'],
                            '_D' : log['_D']
                            }
                    air.append({**vl_row, **log['character']['location']})
            
            
            elif log['_T'] == "LogMatchStart" :
                    start_time = datetime.datetime.strptime(log['_D'], '%Y-%m-%dT%H:%M:%S.%fZ')
            
            # 1등 팀 파싱
            elif log['_T'] == 'LogMatchEnd' :
                tar = log
                rank_first = list(filter(lambda x : x['character']['ranking'] == 1, tar['characters']))
                first = list(map(lambda x : x['character']['accountId'], rank_first))
            
            # 이동경로 관련 파싱
            if 'character' in log.keys() :        
                    
                name = log['character']['accountId'] + 'record_start'
                if name not in locals().keys() :
                    locals()[name] = False
                
                if log['_T'] == 'LogParachuteLanding' :
                    locals()[name] = True
                    
                if locals()[name] :
                    if log['_T'] in wanted_position :
                        created_at = log['_D']
                        small_row = {'account_id' : log['character']['accountId'],
                                        '_T' : log['_T'],
                                        'elapsed_time' : (datetime.datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds,
                                    }
                        position_row = {**small_row, ** log['character']['location']}
                        positions.append(position_row)

                if log['character']['accountId'] not in account_ids and 'ai' not in log['character']['accountId']:
                    account_ids.append(log['character']['accountId'])
            
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

            position_path = f"parquets/positions/{match_id}_position.parquet"
            kv2_path = f"parquets/killv2/{match_id}_killv2.parquet"
            tds_path = f"parquets/tds/{match_id}_tds.parquet"
            pd.DataFrame(positions).to_parquet(position_path, engine = 'pyarrow')
            pd.DataFrame(kv2).to_parquet(kv2_path, engine = 'pyarrow')
            pd.DataFrame(tds).to_parquet(tds_path, engine = 'pyarrow')
            firstX = air[0]['x']
            firstY = air[0]['y']
            lastX = air[len(air) // 2]['x']
            lastY = air[len(air) // 2]['y']
            airplane = {'firstX' : firstX,
                        'firstY' : firstY,
                        'lastX' : lastX,
                        'lastY' : lastY,
                        }
            return {'match_id' : match_id, 'zone' : zone_list, 'airplane' : airplane, 'first' : first, 'account_ids' : account_ids}
        except :
            return {'match_id' : match_id, 'zone' : None, 'airplane' : None, 'first' : None, 'account_ids' : None}
    
    else :
        return {'match_id' : match_id, 'zone' : None, 'airplane' : None, 'first' : None, 'account_ids' : None}


def multiwork_get_game_data(dict_list) :
    pool = Pool()
    result = []
    result.append(pool.map(game_data, dict_list))
    pool.close()
    pool.join()
    return result