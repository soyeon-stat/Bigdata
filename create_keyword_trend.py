import pdb
import json
import requests
import pandas as pd
import numpy as np
from config.config import *
from sqlalchemy import create_engine

def fetch_basic_data() :

    query = {
        "sort": [{"_id": {"order": "asc"}}], 
        "size": 1000, 
        "query" : {
            "match_all" : {},
        },
    }

    response = requests.get(
        url = f"{OPENSEARCH_URL}/modified_basic/_search",
        headers = OPENSEARCH_HEADERS,
        auth = OPENSEARCH_AUTH,
        data = json.dumps(query)
    )
    data = response.json()
    hits = data['hits']['hits']
    
    while True :
        if not hits :
            break
        last_sort_value = hits[-1]['sort']
        query['search_after'] = last_sort_value
        response = requests.get(
            url = f"{OPENSEARCH_URL}/modified_basic/_search",
            headers = OPENSEARCH_HEADERS,
            auth = OPENSEARCH_AUTH,
            data = json.dumps(query)
        )

        if response.status_code == 200 : 
            new_data = response.json()
            new_hits = new_data['hits']['hits']
            if not new_hits :
                break
            hits.extend(new_hits)

    return hits


if __name__ == '__main__' :

    # 1. 데이터 불러오기
    basic_data = pd.DataFrame()

    response = fetch_basic_data()
    basic_cols = ['post_date','community' ,'sentiment']

    for item in response :
        try : 
            buffer = {c : item['_source'].get(c, None) for c in basic_cols}
            buffer = pd.DataFrame(buffer)
            if not buffer.empty : 
                basic_data = pd.concat([basic_data, buffer])
        except :
            continue
 
    # 2. 데이터 전처리
    basic_data['keyword'] = basic_data.index # 키워드칼럼 생성
    basic_data['label'] = basic_data['sentiment'].apply(lambda x : x[0].strip()) # 감성 label 생성
    basic_data['label_score'] = basic_data['sentiment'].apply(lambda x : float(x[1].strip())) # 감성 score 생성
    basic_data['post_date'] = basic_data['post_date'].apply(lambda x : x[:10].strip()) # yyyy-mm-dd 데이터로 생성

    basic_data.reset_index(drop = True, inplace = True) # 인덱스 초기화
    basic_data.drop(['sentiment'], axis = 1, inplace = True) # 감성 칼럼 삭제
    basic_data = basic_data.drop_duplicates()

    basic_data['label'] = basic_data['label'].str.replace("neutral", "중립")
    basic_data['label'] = basic_data['label'].str.replace("negatively", "부정")
    basic_data['label'] = basic_data['label'].str.replace("negative", "부정")
    basic_data['label'] = basic_data['label'].str.replace("부정적", "부정")
    basic_data['label'] = basic_data['label'].str.replace("positively", "긍정")
    basic_data['label'] = basic_data['label'].str.replace("positive", "긍정")
    basic_data['label'] = basic_data['label'].str.replace("긍정적", "긍정")
    basic_data['label'] = basic_data['label'].str.replace("mixed", "혼합")
    basic_data['label'] = basic_data['label'].str.replace("anxious", "불안")


    basic_data = basic_data.pivot_table(index = ['post_date', 'community', 'keyword'], columns = 'label', aggfunc = 'size', fill_value=0)
    basic_data = basic_data.reset_index()
    
    engine = create_engine(f"postgresql+psycopg2://{DB_ID}:{DB_PW}@{DB_URL}:{DB_PORT}/{DB_NAME}")
    cols = ['post_date', 'community', 'keyword', '긍정', '부정','불안', '중립', '혼합']
    basic_data[cols].to_sql('trends', con = engine, if_exists='replace') # DB에 데이터 업데이트