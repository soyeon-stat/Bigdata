import pdb
import pandas as pd
import requests 
import json
import openai
from config.config import *

def fetch_source_data(keyword_list) :
    """
    원천데이터를 불러오는 함수
    단, query부분은 성능 감안하여 개선여부 검토할 예정
    사전에 정한 키워드 목록(keyword_list)을 포함하는 게시글만 불러오는 방향으로
    성능 향상 가능할 듯함
    """
    query = {
        "size" : 10000, 
        "query" : {
            "match_all" : {}
        }
    }

    response = requests.get(
        url = f"{OPENSEARCH_URL}/source/_search",
        headers = OPENSEARCH_HEADERS,
        auth = OPENSEARCH_AUTH,
        data = json.dumps(query)
    )

    return response.json()['hits']['hits']

def check_if_existing_basic(doc_id) :
    url = f'{OPENSEARCH_URL}/basic/_docs/{doc_id}'
    resp = requests.get(
        url = url, 
        auth = OPENSEARCH_AUTH,
    )

    return resp.status_code == 200

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

def get_sentiment_result(keywords, text) :
    """
    GPT API를 사용하여 keyword에 대한 감성분석한 결과를 불러오는 함수
    """

    prompt = """
    You are a professional sentiment analyst.
    Your task is to perform sentiment analysis for a keyword in a community post.
    You will receive the keyword, title, content, and comments for the post.
    If the given keyword is not valid within the context of the post, you must ignore to answer
    If the keyword is valid, your answer must be in the form of 'keyword, label, score;', where the label is the most probable sentiment (positive, negative, or neutral) and the score represents its intensity.
    Each keyword's score can differ from the overall sentiment of the post.
    """

    msg = f"""
    키워드 : {keywords}
    게시글 : {text}
    """

    response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                    {"role" : "system", "content" : prompt},
                    {"role" : "user" , "content" : msg}
                    ])

    result = response.choices[0].message.content

    pdb.set_trace()

    if result != 'not valid' :
        return result
    else : 
        return None

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

    openai.api_key = GPT_API_KEY

    # 1. 키워드 리스트 정의
    keyword_list = ['계엄령'
                , '대통령'
                , '윤석열'
                , '비트코인'
                ,'업비트'
                ,'흑백요리사'
                ,'로제'
                ,'이재명'
                ,'한강'
                ,'코스피'
                ,'백종원']

    # 2. 원천데이터 불러오기
    filtered_source_data = fetch_source_data(keyword_list)

    for jsonData in filtered_source_data :

        item = jsonData['_source']

        # 중복 데이터 여부 체크
        if check_if_existing_basic(item['ID']) :
            continue

        # 3. 인기정도 구하기
        time_diff = pd.to_datetime(item['timestp']) - pd.to_datetime(item['post_date'])
        unit_time = max(time_diff.total_seconds() / (3600 * 24), 0.5)   # 최소값 부여
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

            # 5. 키워드별로 게시글애 대해 감성 분석(GPT 4o-mini)
            sentiment = {}
            result = get_sentiment_result(keywords = keywords, text = text)
            result = result.split(";")
            for v in result :
                k, l, s = v.split(",")
                sentiment[k.strip()] = [l.strip(), s.strip()]


            # for k in keywords :
            #     # +--------------------------------+
            #     # |         Dummy data             |
            #     # +--------------------------------+
            #     # label = random.choice(['positive', 'negative', 'neutral'])
            #     # score = str(random.random())[:6]
            #     # result = [label, score]
            #     # sentiment[k] = result

            if len(sentiment) > 0 :
                item['sentiment'] = sentiment

                # 6. 기초데이터로 업로드 (키워드가 있는 경우만)
                upload_to_basic_data(item['ID'], item)