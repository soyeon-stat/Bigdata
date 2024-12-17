import pdb
import pandas as pd
import requests 
import json
import openai
import re
from tqdm import tqdm
from config.config import *


def fetch_source_data(community) :
    """
    원천데이터를 불러오는 함수. 커뮤니티별로 데이터를 불러옴.
    """
    query = {
        "sort": [{"_id": {"order": "asc"}}], 
        "size": 1000, 
        "query" : {
            "term" : {
                "community.keyword" : community
            }
        },
    }

    response = requests.get(
        url = f"{OPENSEARCH_URL}/source/_search",
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
            url = f"{OPENSEARCH_URL}/source/_search",
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
    You are a sentiment analyst. Analyze the sentiment of a given keyword in a community post.  
    - Labels: positive, negative, neutral, mixed, or anxious.  
    - Score: intensity of the sentiment in a 1 to 10 scale.
    Rules:  
    1. Ignore invalid or irrelevant keywords.  
    2. For valid keywords, respond in this format without punctuation or descriptions: keyword, label, score
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

    if result != 'not valid' :
        return result
    else : 
        return None

def upload_to_basic_data(doc_id, data) :
    """
    기본 데이터로 가공한 결과를 업로드하는 함수
    """
    url = f"{OPENSEARCH_URL}/modified_basic/_doc/{doc_id}"
    response = requests.post(
        url = url,
        headers = OPENSEARCH_HEADERS,
        auth = OPENSEARCH_AUTH,
        data = json.dumps(data)
    )

    assert response.status_code >= 200 and response.status_code < 300

def covert_to_number(string_number) :

    if type(string_number) == str : 
        multiplyer = 1
        if 'k' in string_number or 'K' in string_number :
            multiplyer = 1000
        elif 'm' in string_number or 'M' in string_number :
            multiplyer = 1000000
        num = re.sub(r"[^0-9]", "", string_number)

        return float(num) * multiplyer

    else :
        return string_number


def calculate_popular(numerator, denomenator) :
    
    if numerator is None :
        return None
    
    if denomenator != 0 :
        numerator = covert_to_number(numerator)

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
    community_list = [
        'gasengi',
        'dcinside',
        # 'clien',  # 완료
        # 'theqoo', # 완료
        # 'INSTIZ', # 완료
        # '개드립넷',# 완료
        # 'NATEPANN',  # 완료
        # 'HUMORUIV', # 완료
        # 'mlbpark', 
    ]
    
    for community in community_list : 

        print(community)
        filtered_source_data = fetch_source_data(community)

        for jsonData in tqdm(filtered_source_data) :

            item = jsonData['_source']

            # doc_id 구하기
            doc_id = item.get('ID', "")
            if not doc_id :
                doc_id = item.get('id', "")

            # 중복 데이터 여부 체크
            if check_if_existing_basic(doc_id) :
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


            # 5. 키워드 게시글에 대한 감성 분석(GPT 4o-mini)
            if keywords :
                sentiment = {}
                result = get_sentiment_result(keywords = keywords, text = text)
                result = result.replace(" ", "")
                result = result.split("\n")
                result = [v for v in result if len(v) > 0]
                for v in result :
                    try: 
                        k, l, s = v.split(",")
                        k = k.replace("keyword:", "")
                        l = l.replace("label:", "")
                        s = s.replace("score:", "")
                        sentiment[k.strip()] = [l.strip(), s.strip()]
                    except:
                        continue


                # # +--------------------------------+
                # # |         Dummy data             |
                # # +--------------------------------+
                # for k in keywords :
                #     label = random.choice(['positive', 'negative', 'neutral'])
                #     score = str(random.random())[:6]
                #     result = [label, score]
                #     sentiment[k] = result

                if len(sentiment) > 0 :
                    item['sentiment'] = sentiment

                    # 6. 기초데이터로 업로드 (키워드가 있는 경우만)
                    upload_to_basic_data(doc_id, item)