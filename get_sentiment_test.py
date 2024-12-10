import pdb
import random
import openai
import pandas as pd
from config.config import *

def get_sentiment_result(keyword, text) :
    """
    GPT API를 사용하여 keyword에 대한 감성분석한 결과를 불러오는 함수
    """

    prompt = f"""
    {keyword}에 대해선 "{text}"를 감성분석해줘.
    분석한 결과는 [label, score]의 형태로만 반환해줘
    """

    response = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                    {"role" : "system", "content" : "You are a professional sentiment analyzer."},
                    {"role" : "user" , "content" : prompt}
                    ],
            max_tokens=40,
            temperature=0.3)

    # 응답에서 텍스트 추출
    result = response['choices'][0]['message']['content']

    pdb.set_trace()

    return result


if __name__ == '__main__' : 
    openai.api_key = GPT_API_KEY

    df = pd.read_json('data_with_keywords_v3.json')
    df = df.to_dict(orient='records')

    for post in df :

        keywords = post.get("keywords", "")
        
        title = post.get("title", "")
        content = post.get("content", "")
        comments = post.get("comments", [])
        if len(comments) > 10 :
            comments = random.sample(comments, int(len(comments)/3))
        comments = ",".join(comments)

        text = f"""
            제목 : {title}
            본문 : {content}
            댓글들 : {comments}
"""
        pdb.set_trace()
        # buffer = []
        # for k in keywords :
        #     pdb.set_trace()
        #     sentiment = get_sentiment_result(post, text)
        #     buffer.append(sentiment)