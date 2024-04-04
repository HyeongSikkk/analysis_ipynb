from multiprocessing import Pool

def multi_tool(func, data) :
    result = []
    pool = Pool()
    
    # 결과를 리스트에 추가
    result.append(pool.map(game_data, dict_list))
    # 작업이 완료되면 풀을 종료
    pool.close()
    pool.join()
    return r[0]