import pdb
import pandas as pd
import numpy as np
import random
from config.config import *
from sqlalchemy import create_engine


date_list = pd.date_range('2024-11-01', '2024-12-06')
community_list = ['dcinside', 'gasengi', 'humoruniv', 'instiz', 'natepann',
             'clien', 'dogdrip', 'mlbpark', 'theqoo']
keyword_list = ['계엄령', '대통령', '윤석열', '비트코인','업비트',
            '흑백요리사','로제','이재명','한강','코스피','백종원']

def create_dummy(community, keyword) :

    date_list = pd.date_range('2024-11-01', '2024-12-06')
    
    label = []

    init_label = random.sample(['긍정', '부정', '중립', '혼합', '불안'], k = 1)
    label_population = [init_label[0]]
    for d in date_list :
        sample_label = random.sample(['긍정', '부정', '중립', '혼합', '불안'], k = 1)
        label_population.append(sample_label[0])
        
        
        l = np.random.choice(label_population)
        label.append(l)

    label_score = np.random.rand(len(date_list)) * 10
    label_score = pd.Series(label_score).rolling(window=7, min_periods=1).mean()

    
    count_post = np.random.randint(0, 20, size = len(date_list))
    count_cum_post = pd.Series(count_post).cumsum()
        
    count_like = np.random.rand(len(date_list)) * 10
    count_like = pd.Series(count_like).cumsum()
    avg_count_like = count_like.rolling(window = 7, min_periods=1).mean()

    count_view = np.random.rand(len(date_list)) * 100
    count_view = pd.Series(count_like).cumsum()
    avg_count_view = count_view.rolling(window = 7, min_periods=1).mean()
    
    count_comment = np.random.rand(len(date_list))*50
    count_comment = pd.Series(count_comment).cumsum()
    avg_count_comment = count_comment.rolling(window = 7, min_periods = 1).mean()

    links = {
        'dcinside' : 'https://www.dcinside.com/',
        'gasengi' : 'http://www.gasengi.com/', 
        'humoruniv' : 'https://m.humoruniv.com/main.html', 
        'instiz' : 'https://www.instiz.net/', 
        'natepann' : 'https://pann.nate.com/',
        'clien' : 'https://www.clien.net/service/board/kin', 
        'dogdrip' : 'https://www.dogdrip.net/', 
        'mlbpark' : 'https://mlbpark.donga.com/mp/', 
        'theqoo' : 'https://theqoo.net/'        
    }
    link = [links[community]] * len(date_list)

    keyword_score = count_like + count_view + count_comment

    return pd.DataFrame(
        {
            'date' : date_list,
            'community': [community] * len(date_list),
            'keyword': [keyword] * len(date_list),
            'keyword_score': keyword_score,
            'label': label,
            'label_score': label_score,
            'count_post': count_post,
            'count_cum_post': count_cum_post,
            'count_like': count_like,
            'avg_count_like': avg_count_like,
            'count_view': count_view,
            'avg_count_view': avg_count_view,
            'count_comment': count_comment,
            'avg_count_comment': avg_count_comment,
            'link' : link,
        }
    )



if __name__ == '__main__' :

    dashboard = pd.DataFrame()

    for community in community_list :
        for keyword in keyword_list :
            dummy = create_dummy(community, keyword)
            dashboard = pd.concat([dashboard, dummy])

    engine = create_engine(f"postgresql+psycopg2://{DB_ID}:{DB_PW}@{DB_URL}:{DB_PORT}/{DB_NAME}")
    dashboard.to_sql('dashboard', con = engine, if_exists='replace') # DB에 데이터 업데이트