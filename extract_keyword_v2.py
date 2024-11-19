import pdb
from keybert import KeyBERT
import re
from pykospacing import Spacing
from kiwipiepy import Kiwi
from konlpy.tag import Okt
from soynlp.normalizer import emoticon_normalize, repeat_normalize

"""
[source]
Spacing : https://github.com/haven-jeon/PyKoSpacing.git
"""

spacing = Spacing()
kiwi = Kiwi()
model = KeyBERT()
okt = Okt()

def clean_doc(doc) :

    cln_sent = spacing(doc)                                  # 띄어쓰기
    cln_sent = emoticon_normalize(cln_sent, num_repeats=2)   # 이모티콘 표준화
    cln_sent = repeat_normalize(cln_sent, num_repeats=2)     # 반복내용 표준화
    cln_sent = re.sub(r'[^\w*\s*]', "", cln_sent)            # 문자, 숫자가 아닌 경우 삭제

    return cln_sent

if __name__ == '__main__' :
    
    # docs = '이 제품은 디자인은좋으나 성능이별로다. 특히 가격은 더 좋지 않다. 연락처) 010-7566-1742. 앜ㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋ'
    # docs = '걍 슴천지들이 이때다하고 괜히 채원 들먹이며 나대는거지뭐'
    # docs = "중소는 활동을 안해... 대형이 훠얼씬 좋아ㅠ"
    # docs = "너무나 연예인인데.... 연예인이랑 일반인 기준은 다르다고 치고 그럼 너도 살찌고 거울보면 음 좀 사람같지않네....이런 마음 들어???"
    docs = "예전 뼈말라보다 지금이 더 나은데 살 더 쪄도 예쁠거같음"
    cln_doc = clean_doc(docs)
    extraced_keyword = model.extract_keywords(cln_doc)
    filter_keyword = [k for k, s in extraced_keyword if s > 0.5]


    print(f"원래 문서 : {docs}")
    print(f"정제된 문서 : {cln_doc}")
    print(f"추출한 키워드 : {extraced_keyword}")
    print(f"정제한 키워드 : {filter_keyword}")
    print()
    pdb.set_trace()
