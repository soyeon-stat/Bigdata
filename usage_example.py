from config.config import *
from sqlalchemy import create_engine
import pandas as pd

# 데이터 불러오는 예제
engine = create_engine(f"postgresql+psycopg2://{DB_ID}:{DB_PW}@{DB_URL}:{DB_PORT}/{DB_NAME}")

if __name__ == '__main__' : 
    data = pd.read_sql("SELECT * FROM dashboard", engine).drop('index', axis = 1)
    print(data)
