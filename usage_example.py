from config.config import *
from sqlalchemy import create_engine
import pandas as pd

if __name__ == '__main__' : 
    engine = create_engine(f"postgresql+psycopg2://{DB_ID}:{DB_PW}@{DB_URL}:{DB_PORT}/{DB_NAME}")

    query = """
    SELECT *
    FROM dashboard
    """

    data = pd.read_sql(query, engine).drop('index', axis = 1)
    print(data)
