import re
import requests
from konlpy.tag import Okt
import pandas as pd
import random
from tqdm import tqdm


okt = Okt()
min_length = 2
stop_word = ["은", "는", "이", "가", "시발", "일링", "존내", "그냥", "저만큼",
             "이기", "이번", "돌이", "뉴규", "어쩌", "자마자"]

def load_json_data(url) :
    response = requests.get(url)
    return response.json()

def make_input_data(content) :
    style = content['community']
    comments = content['comments']

    buffer = []
    for text in comments :
        clean_text = clean_doc(text)
        nouns = okt.nouns(clean_text)
        # 중복 제거, 최대 5개만 추출
        nouns = list(set(nouns))[:5]
        input_text = clean_noun(nouns)
        
        if len(input_text) >= 2 :
            # 정제한 결과 내용물이 있으면 데이터 추가
            input_text = ",".join(input_text)
            buffer.append((input_text, style, text))

    return pd.DataFrame(buffer, columns = ['input_text', 'style', 'text'])

def clean_doc(doc) :
    doc = re.sub(r'[^\w\s]', " ", doc)
    doc = doc.replace("\xa0", " ") # 공백문자 인코딩 깨진 경우
    
    return doc

def clean_noun(text, min_length = 2, stop_word = stop_word) : 
    text = [x for x in text if len(x) >= min_length]
    text = [x for x in text if x not in stop_word]
    return text


if __name__ == '__main__' :

    urls = [
        'https://raw.githubusercontent.com/soyeon-stat/Bigdata/refs/heads/master/INSTIZ_CONTENTS.json',
        'https://raw.githubusercontent.com/soyeon-stat/Bigdata/refs/heads/master/HUMOR_CONTENTS.json'
        ]

    result = pd.DataFrame()   
    for url in urls :

        data = load_json_data(url)
        df = pd.DataFrame()
        for content in tqdm(data) :
            clean_data = make_input_data(content)
            df = pd.concat([df, clean_data])
        result = pd.concat([result, df])

    # 결과 저장
    # 20% : validation / 80% training
    result.reset_index(inplace = True, drop = True)
    smpl = random.sample(list(result.index), int(len(result)*0.2))
    result.loc[smpl, ['input_text', 'style', 'text']].to_json('data/validation.jsonl', orient='records', lines = True, force_ascii=False)
    result.drop(smpl)[['input_text', 'style', 'text']].to_json('data/train.jsonl', orient='records', lines = True, force_ascii=False)