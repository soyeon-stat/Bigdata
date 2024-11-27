import pdb
import pandas as pd
import requests 
import json
import openai
from config.config import *
import random       # 2024.11.27. 테스트용으로만 사용. 이후 삭제 예정


openai.api_key = GPT_API_KEY

def fetch_source_data(keyword_list) :
    """
    원천데이터를 불러오는 함수
    단, query부분은 성능 감안하여 개선여부 검토할 예정
    사전에 정한 키워드 목록(keyword_list)을 포함하는 게시글만 불러오는 방향으로
    성능 향상 가능할 듯함
    """
    query = {
        "size" : 1000, # 불러올 데이터의 양. 디버그가 끝난 이후 늘릴 예정
        "query" : {
            "match_all" : {} # 모든 데이터를 조회
        }
    }

    response = requests.get(
        url = f"{OPENSEARCH_URL}/source/_search",
        headers = OPENSEARCH_HEADERS,
        auth = OPENSEARCH_AUTH,
        data = json.dumps(query)
    )

    return response.json()['hits']['hits']


def check_which_keyword(item, keyword_list) :
    """
    게시글이 키워드 목록 중에 어떤 키워드를 포함하고 있는지 체크해주는 함수
    """
    text = f"{item['title']}, {item['content']}, {','.join(item['comments'])}"

    keywords = [k for k in keyword_list if k in text]

    if len(keywords) == 0 :
        return None
    else : 
        return keywords

def get_sentiment_result(keyword, text) :
    """
    GPT API를 사용하여 keyword에 대한 감성분석한 결과를 불러오는 함수
    """

    prompt = f"""
    {keyword}에 대해선 "{text}"를 감성분석해줘.
    분석한 결과는 [label, score]의 형태로만 반환해줘
    """

    response = openai.Completion.create(
            engine="gpt-4",  # 사용할 모델 (GPT-3.5 이상 사용 가능)
            prompt=prompt,   # 모델에 전달할 텍스트 프롬프트
            max_tokens=40,   # 최대 토큰 수 (응답의 길이)
            temperature=0.3)
        
    # 응답에서 텍스트 추출
    message = response.choices[0].text.strip()

    return message

def upload_to_basic_data(doc_id, data) :
    """
    기본 데이터로 가공한 결과를 업로드하는 함수
    """
    url = f"{OPENSEARCH_URL}/basic/_doc/{doc_id}"
    response = requests.post(
        url = url,
        headers = OPENSEARCH_HEADERS,
        auth = OPENSEARCH_AUTH,
        data = json.dumps(data)
    )

    assert response.status_code >= 200 and response.status_code < 300

def calculate_popular(numerator, denomenator) :
    if numerator is None :
        return None
    if denomenator != 0 :
        return float(numerator) / denomenator

if __name__ == '__main__' :

    # 1. 키워드 리스트 정의
    keyword_list = ['화이팅', '취향']

    # 2. 원천데이터 불러오기
    filtered_source_data = fetch_source_data(keyword_list)
    
    for jsonData in filtered_source_data :

        item = jsonData['_source']

        # 3. 인기정도 구하기
        time_diff = pd.to_datetime(item['timestp']) - pd.to_datetime(item['post_date'])
        unit_time = time_diff.total_seconds() / (3600 * 24)
        item['vote_level'] = calculate_popular(item['vote_up'], unit_time)
        item['view_level'] = calculate_popular(item['view'], unit_time)
        item['comment_level'] = calculate_popular(item['n_comment'], unit_time)

        # 4. 게시글이 어떤 키워드를 포함하는지 확인
        text = f"""
            제목 : {item['title']}
            본문 : {item['content']}
            댓글들 : {','.join(item['comments'])}
            """
        keywords = check_which_keyword(item, keyword_list)

        if keywords :

            # 5. 게시글이 포함하는 키워드별로 감성 분석
            sentiment = {}
            for k in keywords :
                # +-------------------------------+
                # | GPT API KEY 발급 받은 후 실행  |
                # +-------------------------------+
                # result = get_sentiment_result(keyword = k, text = text)
                # sentiment[k] = result

                # +--------------------------------+
                # | API KEY 발급 전 사용할 대체 코드 |
                # | (랜덤으로 label, score 부여)    |
                # +--------------------------------+
                label = random.choice(['긍정', '부정', '중립'])
                score = str(random.random())[:6]
                result = [label, score]
                sentiment[k] = result
            item['sentiment'] = sentiment

            # 6. 기초데이터로 업로드 (키워드가 있는 경우만)
            upload_to_basic_data(item['ID'], item)