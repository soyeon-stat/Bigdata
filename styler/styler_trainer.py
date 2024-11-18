from transformers import T5Tokenizer, AutoModelForSeq2SeqLM, Seq2SeqTrainingArguments, Seq2SeqTrainer, AutoTokenizer
from datasets import load_dataset

def preprocess_function(batch):
    batch["input"] = batch["input_text"] + " [STYLE] " + batch["style"]
    batch["output"] = batch["text"]
    return batch

def tokenize_function(batch):
    tokenized_input = tokenizer(batch["input"], max_length=128, truncation=True, padding="max_length")
    tokenized_output = tokenizer(batch["output"], max_length=128, truncation=True, padding="max_length")
    tokenized_input["labels"] = tokenized_output["input_ids"]
    return tokenized_input

if __name__ == '__main__' : 

    # 1. 데이터셋
    dataset = load_dataset("json", data_files={"train": "data/train.jsonl", "validation": "data/validation.jsonl"})
    dataset = dataset.map(preprocess_function, remove_columns=["input_text", "style", "text"])

    # 2. 모델 및 토크나이저 설정
    model_name = "hyunwoongko/kobart" # KoBart를 사용하는 경우
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSeq2SeqLM.from_pretrained(model_name).to("cuda")

    # 3. 토큰화
    tokenized_datasets = dataset.map(tokenize_function, batched=True, remove_columns=["input", "output"])

    # 4. 학습 설정
    training_args = Seq2SeqTrainingArguments(
        output_dir="./style_transfer_model",
        evaluation_strategy="steps",
        eval_steps=500,
        logging_dir="./logs",
        logging_steps=100,
        save_strategy="epoch",
        save_total_limit=2,
        learning_rate=1e-5,
        max_grad_norm = 1.0,
        per_device_train_batch_size=16,  # GPU 메모리
        per_device_eval_batch_size=16,   # GPU 메모리
        gradient_accumulation_steps=2,
        num_train_epochs=3,
        weight_decay=0.01,
        predict_with_generate=True,
        fp16=True  # FP16 혼합 정밀도 활성화
    )

    # 4. Trainer 설정
    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_datasets["train"],
        eval_dataset=tokenized_datasets["validation"],
        tokenizer=tokenizer
    )

    # 5. 모델 학습
    trainer.train()

    # 6. 모델 저장
    trainer.save_model("./style_transfer_model")
    tokenizer.save_pretrained("./style_transfer_model")