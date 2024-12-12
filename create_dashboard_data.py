import pdb
import json
import requests
import pandas as pd
import numpy as np
from config.config import *
from sqlalchemy import create_engine

def fetch_basic_data() :
    query = {
        'size' : 1000,
        'query' : {
            'match_all' : {},
        }
    }
    response = requests.get(
        url =  f"{OPENSEARCH_URL}/basic/_search",
        headers = OPENSEARCH_HEADERS,
        auth = OPENSEARCH_AUTH,
        data = json.dumps(query)
    )

    return response.json()['hits']['hits']

def calcurate_period_popular(group, col, agg, window_size) :
    group = group.reset_index()
    group = group.sort_values('post_date')
    group = group.groupby(['community', 'keyword'])
    if agg == 'mean' : 
        return group[col].rolling(window = window_size, min_periods=1).mean()
    elif agg == 'sum' : 
        return group[col].rolling(window = window_size, min_periods=1).sum()

if __name__ == '__main__' :

    # 1. 데이터 불러오기
    basic_data = pd.DataFrame()

    response = fetch_basic_data()
    basic_cols = ['post_date', 'community', 'vote_level', 'view_level', 'comment_level', 'sentiment']

    for item in response :
        buffer = {c : item['_source'][c] for c in basic_cols}
        buffer = pd.DataFrame(buffer)
        basic_data = pd.concat([basic_data, buffer])

    # 2. 데이터 전처리
    basic_data['keyword'] = basic_data.index # 키워드칼럼 생성
    basic_data['label'] = basic_data['sentiment'].apply(lambda x : x[0].strip()) # 감성 label 생성
    basic_data['label_score'] = basic_data['sentiment'].apply(lambda x : float(x[1].strip())) # 감성 score 생성
    basic_data['post_date'] = basic_data['post_date'].apply(lambda x : x[:10].strip()) # yyyy-mm-dd 데이터로 생성
    basic_data['count_post'] = 1

    basic_data.reset_index(drop = True, inplace = True) # 인덱스 초기화
    basic_data.drop(['sentiment'], axis = 1, inplace = True) # 감성 칼럼 삭제


    # 3. 주요 지표 계산
    pivoting = basic_data.groupby(['community', 'keyword', 'post_date'])

    # 주요 지표를 계산할 기간. 최근 window_size일의 지표를 계산함. 본 데이터에서는 7일로 설정할 예정
    window_size = 2

    # 키워드의 감성 label
    # 감성이 여러개인 경우 중립을 선택. 시간 여유가 된다면 점수가 높은 경우를 택하는 로직으로 변경할 예정
    label = pivoting[['label']].agg(pd.Series.mode)
    label['label'] = [l if type(l) == str else '중립' for l in label['label']] 
    
    # 키워드의 감성 score
    label_score = pivoting[['label_score']].mean()

    # 게시글수/누적게시글수
    count_post = pivoting[['count_post']].sum()
    count_post['count_cum_post'] = calcurate_period_popular(count_post, 'count_post', 'sum', window_size).tolist()

    # 추천수준/평균추천수준
    count_like = pivoting[['vote_level']].mean()
    count_like.columns = ['count_like']
    count_like['avg_count_like'] = calcurate_period_popular(count_like, 'count_like', 'mean', window_size).tolist()

    # 조회수준/평균조회수준
    count_view = pivoting[['view_level']].mean()
    count_view.columns = ['count_view']
    count_view['avg_count_view'] = calcurate_period_popular(count_view, 'count_view', 'mean', window_size).tolist()

    # 댓글수준/평균댓글수준
    count_comment = pivoting[['comment_level']].mean()
    count_comment.columns = ['count_comment']
    count_comment['avg_count_comment'] = calcurate_period_popular(count_comment, 'count_comment', 'mean', window_size).tolist()

    # 4. 데이터 병합    
    dashboard = pd.merge(label.reset_index(), label_score.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_post.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_like.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_view.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_comment.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')

    # 키워드 점수 계산 (단순 합산)
    dashboard['keyword_score'] = dashboard[['avg_count_like', 'avg_count_view', 'avg_count_comment']].sum(axis = 1)

    

    # 5. 데이터 후처리
    dashboard = dashboard.apply(lambda x : round(x, 4))

    # 6. 데이터 업로드
    engine = create_engine(f"postgresql+psycopg2://{DB_ID}:{DB_PW}@{DB_URL}:{DB_PORT}/{DB_NAME}")

    # 데이터 정렬 후 업데이트
    dashboard_cols = ['post_date', 'community','keyword', 'keyword_score',
                      'label', 'label_score',
                      'count_post', 'count_cum_post',
                      'count_like', 'avg_count_like', 
                      'count_view','avg_count_view', 
                      'count_comment','avg_count_comment']
    
    dashboard[dashboard_cols].to_sql('dashboard', con = engine, if_exists='replace') # DB에 데이터 업데이트