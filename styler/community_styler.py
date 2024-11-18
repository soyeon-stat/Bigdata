import pdb
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

# 학습한 모델 로드
model_dir = "./style_transfer_model"
tokenizer = AutoTokenizer.from_pretrained(model_dir)
model = AutoModelForSeq2SeqLM.from_pretrained(model_dir)

# 문체 변환 수행
def style_transfer(input_text, target_style):
    input_text = input_text + " [STYLE] " + target_style
    inputs = tokenizer(input_text, return_tensors="pt", truncation=True)
    outputs = model.generate(**inputs, max_length=128)
    return tokenizer.decode(outputs[0], skip_special_tokens=True)

if __name__ == '__main__' :
    while True : 
        input_text = input("입력하고 싶은 내용 입력(명사로만 입력) : ")
        if input_text == '99' :
            break
        target_style = input("커뮤니티 스타일 : ")
        result = style_transfer(input_text, target_style)
        print(result)
        pdb.set_trace()