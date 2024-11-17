from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from transformers import BertTokenizer, BertForSequenceClassification

class BERT_sentiment :

    def __init__(self, model) :

        self.sentiment_analyzer = None
        self.model = model
        self.model_dict = {
            'kcBERT' : "beomi/KcBERT-base",
            'multilingual' : "nlptown/bert-base-multilingual-uncased-sentiment",
            'koBERT' : "monologg/kobert",
            'KoELECTRA' : "monologg/koelectra-base-v3-discriminator",
            'klueBERT' : "klue/bert-base",
            'KorBERTa' : "kykim/bert-kor-base"
            }

        if self.model  == 'koBERT' :
            self.load_model_koBERT(self.model)
        else :
            self.load_model(self.model)


    def load_model(self, model) :
        model_name = self.model_dict[model]
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.sentiment_analyzer = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

    def load_model_koBERT(self, model) :
        model_name = self.model_dict[model]
        tokenizer = BertTokenizer.from_pretrained(model_name)
        model = BertForSequenceClassification.from_pretrained(model_name)
        self.sentiment_analyzer = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)


if __name__ == '__main__' :


    models = ['koBERT', 'kcBERT', 'KoELECTRA', 'klueBERT', 'KorBERTa', 'multilingual']

    for m in models :

        # 1. 모델 로드
        model = BERT_sentiment(m)
        sentiment_analyzer = model.sentiment_analyzer

        # 2. 분석할 문장과 키워드 정의
        sentence = "이 제품은 디자인은 좋지만, 성능은 별로고 가격이 비싸다."
        keywords = ["디자인", "성능", "가격"]

        # 3. 키워드별 감성 분석 수행
        results = {}
        for keyword in keywords:
            # 각 키워드 중심으로 문장을 재구성
            keyword_sentence = f"{keyword}에 대해서 말하자면, {sentence}"
            # 감성 분석 수행
            analysis = sentiment_analyzer(keyword_sentence)[0]
            # 결과 저장
            results[keyword] = {
                "label": analysis["label"],
                "score": analysis["score"]
            }

        # 5. 결과 출력
        print(f"----- {m} 감성분석 결과 -----")
        for keyword, analysis in results.items():
            print(f"키워드: {keyword}, 감성: {analysis['label']}, 확신도: {analysis['score']:.2f}")


