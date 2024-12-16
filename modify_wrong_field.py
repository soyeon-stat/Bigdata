import pdb
import pandas as pd
import requests 
import json
from tqdm import tqdm
import re
from config.config import *


def fetch_data(community, domain) :

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
        url = f"{OPENSEARCH_URL}/basic/_search",
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
            url = f"{OPENSEARCH_URL}/basic/_search",
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

def get_source_doc(doc_id) :
    url = f'{OPENSEARCH_URL}/source/_search'

    query = {
        "query" : {
            "term" : {
                "ID.keyword": doc_id
                }
            }
    }

    resp = requests.get(
        url = url, 
        auth = OPENSEARCH_AUTH,
        headers = OPENSEARCH_HEADERS,
        data = json.dumps(query)
    )
    hits = resp.json()['hits']['hits']
    if len(hits) > 0 :
        return resp

    else :
        query = {
            "query" : {
                "term" : {
                    "id.keyword": doc_id
                    }
                }
        }
        resp = requests.get(
            url = url, 
            auth = OPENSEARCH_AUTH,
            headers = OPENSEARCH_HEADERS,
            data = json.dumps(query)
        )
        return resp.json()['hits']['hits']



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


def upload_to_basic_data(doc_id, data) :
    url = f"{OPENSEARCH_URL}/modified_basic/_doc/{doc_id}"
    response = requests.post(
        url = url,
        headers = OPENSEARCH_HEADERS,
        auth = OPENSEARCH_AUTH,
        data = json.dumps(data)
    )
    assert response.status_code >= 200 and response.status_code < 300


if __name__ == '__main__' : 

    community_list = [
        'clien',  # 완료
        'theqoo', # 완료
        'INSTIZ', # 완료
        '개드립넷', # 완료
        'NATEPANN', # 완료
        'HUMORUIV', # 완료
    ]
    for community in community_list : 
        print(community)
        basic = fetch_data(community, 'basic')

        for jsonData in tqdm(basic) :
            item = jsonData['_source']

            # doc_id 구하기
            doc_id = item.get('ID', "")
            if not doc_id :
                doc_id = item.get('id', "")


            source_item = get_source_doc(doc_id)
            source_item = source_item[0]['_source']
            time_diff = pd.to_datetime(source_item['timestp']) - pd.to_datetime(source_item['post_date'])
            unit_time = max(time_diff.total_seconds() / (3600 * 24), 0.5)   # 최소값 부여

            item['vote_level'] = calculate_popular(source_item['vote_up'], unit_time)
            item['view_level'] = calculate_popular(source_item['view'], unit_time)
            item['comment_level'] = calculate_popular(source_item['n_comment'], unit_time)

            upload_to_basic_data(doc_id, item)