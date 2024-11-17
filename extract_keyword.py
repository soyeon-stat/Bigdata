import pdb
import re
import requests
from krwordrank.word import KRWordRank
from kiwipiepy import Kiwi
from konlpy.tag import Hannanum
import gensim
import pandas as pd

def clean_doc(doc) :
    doc = re.sub(r'[^\w\s]', " ", doc)

    # 인코딩 깨진 경우
    doc = doc.replace("\xa0", " ") # 공백문자

    return doc

if __name__ == '__main__' : 
    # 1.데이터 불러오기
    url = 'https://raw.githubusercontent.com/soyeon-stat/Bigdata/refs/heads/master/INSTIZ_CONTENTS.json'
    response = requests.get(url)
    jsonData = response.json()
    sentence = jsonData[0]['content']

    # 2. 문장 나누기
    kiwi = Kiwi()
    text_list = []
    for sent in kiwi.split_into_sents(sentence) :
        _sent = clean_doc(sent.text)
        text_list.append(_sent)

    # 3. 핵심어 추출하기
    # 1) 말뭉치(corpus) 생성
    han = Hannanum()
    corpus = [clean_doc(x) for x in text_list]
    tokenized_corpus = [
        han.nouns(doc) for doc in corpus     # 명사로 추출
    ]

    print('====== 원본 글 ======')
    for x in corpus : 
        print(x)

    # 2) KRWordRank
    min_count = 2   # 단어의 최소 출현 빈도수 (그래프 생성 시)
    max_length = 10 # 단어의 최대 길이
    wordrank_extractor = KRWordRank(min_count=min_count, max_length=max_length)
    beta = 0.85    # PageRank의 decaying factor beta
    max_iter = 20
    keywords, rank, graph = wordrank_extractor.extract(corpus, beta, max_iter)
    print('==== KRWordRank를 사용한 핵심어 ====')
    print(keywords)

    # 3) TF-IDF
    lexicon = gensim.corpora.Dictionary(tokenized_corpus)
    tfidf = gensim.models.TfidfModel(dictionary=lexicon, normalize = True)

    lexicon_dict = pd.DataFrame([x for x in lexicon.items()], columns = ['label', 'name'])
    lexicon_dict = lexicon_dict.set_index('label')

    print("====== TF-IDF ======")
    doc_tfidf = pd.DataFrame()
    for doc in tokenized_corpus :
        vec = lexicon.doc2bow(doc)
        vec = tfidf[vec]
        a = pd.DataFrame(vec, columns = ['label', 'score']) # 데이터프레임 생성
        doc_tfidf = pd.concat([doc_tfidf, a])

    doc_tfidf = doc_tfidf.groupby('label')[['score']].mean().sort_values('score', ascending=False)
    print(doc_tfidf.join(lexicon_dict))


    # 4) Key-BERT
    
    print("====== Key BERT ======")
    from keybert import KeyBERT
    model = KeyBERT()
    # 전체 문서를 기준으로
    nouns = [",".join(x) for x in tokenized_corpus]
    nouns = ",".join(nouns)
    for x in corpus :
        print(x)
    print(model.extract_keywords(nouns))