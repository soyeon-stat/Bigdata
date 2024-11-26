import pandas as pd
import requests 
import json
import openai
from config import *
from datetime import datetime

openai.api_key = GPT_API_KEY

def fetch_source_data(keyword_list) :
    """
    원천데이터를 불러오는 함수
    단, query부분은 성능 감안하여 개선여부 검토할 예정
    사전에 정한 키워드 목록(keyword_list)을 포함하는 게시글만 불러오는 방향으로
    성능 향상 가능할 듯함
    """
    url = "" # 원천 데이터를 저장한 opensearch url

    query = {
    }

    response = requests.get(
        url = url,
        headers = OPENSEARCH_HEADERS,
        auth = OEPNSEARCH_AUTH,
        data = json.dumps(query)
    )

    return response.json()


def check_have_keyword(item, keyword_list) :
    """
    게시글이 키워드 목록 중에 어떤 키워드를 포함하고 있는지 체크해주는 함수
    """

    text = f"{item['title']}, {item['content']}, {",".join(item['comment'])}"

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
    분석한 결과는 (label, score)의 형태로만 반환해줘
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
        auth = OEPNSEARCH_AUTH,
        data = json.dumps(data)
    )
    assert response.status_code >= 200 and response.status_code < 300

if __name__ == '__main__' :

    keyword_list = ['비트코인', '수능']

    filtered_source_data = fetch_source_data(keyword_list)

    for item in filtered_source_data :

        # 1. 인기정도 구하기
        time_diff = pd.to_datetime(item['timestp']) - pd.to_datetime(item['post_date'])
        unit_time = time_diff.total_seconds() / (3600 * 24)
        item['vote_level'] = item['vote_up'] / unit_time
        item['view_level'] = item['view'] / unit_time
        item['comment_level'] = item['n_comment']/unit_time

        # 2. 게시글별 감성분석 결과 구하기
        text = f"""
            제목 : {item['title']}
            본문 : {item['content']}
            댓글들 : {",".join(item['comment'])}
            """
        keywords = check_have_keyword(item, keyword_list)

        if keywords :
            sentiment = {}
            for k in keywords :
                result = get_sentiment_result(keyword = k, text = text)
                sentiment[k] = result
            item['sentiment'] = sentiment

        
        #3. 업로드
        upload_to_basic_data(item['ID'], item)