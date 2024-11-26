import pdb
import requests
import pandas as pd
from config import *


def fetch_basic_data() :
    url = ""
    response = requests.get(
        url = url,
        headers = OPENSEARCH_HEADERS,
        auth = OEPNSEARCH_AUTH,
    )

    return response.json()

def create_engine(db) :
    try :
        yield db

    finally :
        db.close()

if __name__ == '__main__' :

    # 데이터 불러오기
    basic_data = fetch_basic_data()

    # 기초 전처리
    basic_data = pd.DataFrame(basic_data)
    basic_data = basic_data.loc[:, ['post_date', 'community', 'vote_level', 'view_level', 'comment_level', 'sentiment']]
    basic_data['keyword'] = basic_data.index
    basic_data['sentiment_label'] = basic_data['sentiment'].apply(lambda x : x[0])
    basic_data['sentiment_score'] = basic_data['sentiment'].apply(lambda x : x[1])

    # 그룹화
    pivoting = basic_data.groupby(['post_date', 'community', 'keyword'])
    
    # 주요 지표 계산
    label = pivoting['sentiment_label'].mode()
    label_score = pivoting['sentiment_score'].mean()
    count_post = pivoting['post_date'].nunique()
    count_like = pivoting['vote_level'].mean()
    count_view = pivoting['view_level'].mean()
    count_comment = pivoting['comment_level'].mean()

    # 데이터 병합    
    dashboard = pd.merge(label.reset_index(), label_score.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_post.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_like.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_view.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')
    dashboard = pd.merge(dashboard, count_comment.reset_index(), on = ['post_date', 'community', 'keyword'], how = 'left')


    # 데이터 업로드
    engine = create_engine() # DB 연결자 생성
    dashboard.to_sql('dashboad', con = engine, if_exists='replace') # DB에 데이터 업데이트