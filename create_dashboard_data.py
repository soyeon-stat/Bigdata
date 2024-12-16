import pdb
import json
import requests
import pandas as pd
import numpy as np
from config.config import *
from sqlalchemy import create_engine

def fetch_basic_data() :
    """
    원천데이터를 불러오는 함수. 커뮤니티별로 데이터를 불러옴.
    """
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


def calcurate_period_popular(group, col, agg, window_size) :
    group = group.reset_index()
    group = group.sort_values('post_date')
    group = group.groupby(['community', 'keyword'])
    if agg == 'mean' : 
        return group[col].rolling(window = window_size, min_periods=1).mean()
    elif agg == 'sum' : 
        return group[col].rolling(window = window_size, min_periods=1).sum()

def aggregate_basic_data(data, col_nm, idx_col, result_col) :
    l = data[col_nm].quantile(0.05)
    u = data[col_nm].quantile(0.95)
    data = data.loc[(l <= data[col_nm]) & (data[col_nm] <= u), [col_nm] + idx_col]

    pivoting = data.groupby(idx_col)
    
    cnt = pivoting.median()
    cnt.columns = [result_col]
    cnt[f'avg_{result_col}'] = calcurate_period_popular(cnt, result_col, 'mean', window_size).tolist()

    return cnt




if __name__ == '__main__' :

    # 1. 데이터 불러오기
    basic_data = pd.DataFrame()

    response = fetch_basic_data()
    basic_cols = ['post_date', 'community', 'vote_level', 'view_level', 'comment_level', 'sentiment', 'title','link']

    for item in response :
        try : 
            buffer = {c : item['_source'].get(c, None) for c in basic_cols}
            buffer = pd.DataFrame(buffer)
            basic_data = pd.concat([basic_data, buffer.dropna()])
        except :
            continue

    # 2. 데이터 전처리
    basic_data = basic_data.drop_duplicates()
    basic_data['keyword'] = basic_data.index # 키워드칼럼 생성
    basic_data['label'] = basic_data['sentiment'].apply(lambda x : x[0].strip()) # 감성 label 생성
    basic_data['label_score'] = basic_data['sentiment'].apply(lambda x : float(x[1].strip())) # 감성 score 생성
    basic_data['post_date'] = basic_data['post_date'].apply(lambda x : x[:10].strip()) # yyyy-mm-dd 데이터로 생성
    basic_data['count_post'] = 1

    basic_data.reset_index(drop = True, inplace = True) # 인덱스 초기화
    basic_data.drop(['sentiment'], axis = 1, inplace = True) # 감성 칼럼 삭제


    # 3. 주요 지표 계산
    idx_col = ['community', 'keyword', 'post_date']
    pivoting = basic_data.groupby(idx_col)

    # 4. 조회수 높은 링크
    link = basic_data.loc[pivoting['view_level'].idxmax().dropna(), ['community', 'keyword', 'post_date', 'link']]
    title = basic_data.loc[pivoting['view_level'].idxmax().dropna(), ['community', 'keyword', 'post_date', 'title']]


    # 주요 지표를 계산할 기간. 최근 window_size일의 지표를 계산함. 본 데이터에서는 7일로 설정할 예정
    window_size = 5

    # 키워드의 감성 label
    # 감성이 여러개인 경우 중립을 선택. 시간 여유가 된다면 점수가 높은 경우를 택하는 로직으로 변경할 예정
    label = pivoting[['label']].agg(pd.Series.mode)
    label['label'] = [l if type(l) == str else '중립' for l in label['label']] 
    
    # 키워드의 감성 score
    label_score = pivoting[['label_score']].mean()

    # 게시글수/누적게시글수
    count_post = pivoting[['count_post']].sum()
    count_post['count_cum_post'] = calcurate_period_popular(count_post, 'count_post', 'sum', window_size).tolist()

    # (평균)추천/ (평균)조회/ (평균)댓글 수준
    count_like = aggregate_basic_data(basic_data, 'vote_level', idx_col, 'count_like')
    count_view = aggregate_basic_data(basic_data, 'view_level', idx_col, 'count_view')
    count_comment = aggregate_basic_data(basic_data, 'comment_level', idx_col, 'count_comment')

    # count_like = pivoting[['vote_level']].agg(pd.Series.mode)
    # count_like.columns = ['count_like']
    # count_like['avg_count_like'] = calcurate_period_popular(count_like, 'count_like', 'mean', window_size).tolist()

    # # 조회수준/평균조회수준
    # count_view = pivoting[['view_level']].agg(pd.Series.mode)
    # count_view.columns = ['count_view']
    # count_view['avg_count_view'] = calcurate_period_popular(count_view, 'count_view', 'mean', window_size).tolist()

    # # 댓글수준/평균댓글수준
    # count_comment = pivoting[['comment_level']].agg(pd.Series.mode)
    # count_comment.columns = ['count_comment']
    # count_comment['avg_count_comment'] = calcurate_period_popular(count_comment, 'count_comment', 'mean', window_size).tolist()

    # 4. 데이터 병합    
    dashboard = pd.merge(label.reset_index(), label_score.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_post.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_like.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_view.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_comment.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, link.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, title.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')

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
                      'count_comment','avg_count_comment', 'link', 'title']
    
    dashboard['label'] = dashboard['label'].str.replace("neutral", "중립")
    dashboard['label'] = dashboard['label'].str.replace("negative", "부정")
    dashboard['label'] = dashboard['label'].str.replace("positive", "긍정")
    dashboard['label'] = dashboard['label'].str.replace("mixed", "혼합")
    dashboard['label'] = dashboard['label'].str.replace("anxious", "불안")

    dashboard[dashboard_cols].to_sql('dashboard', con = engine, if_exists='replace') # DB에 데이터 업데이트