import psycopg2
import pandas as pd
import warnings
import requests
from requests import get
from bs4 import BeautifulSoup
import pyarrow.parquet
import json
import sqlalchemy
from sqlalchemy.sql import text as sa_text

#warnings.filterwarnings('ignore')

#con = psycopg2.connect(host='34.173.103.16', database='postgres',   user='junior', password='|?7LXmg+FWL&,2(')
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:postgres@localhost/bix')
eng = sqlalchemy.create_engine('postgresql+psycopg2://junior:|?7LXmg+FWL&,2(@34.173.103.16/postgres')

def extract_vendas():
    #con = psycopg2.connect(host='34.173.103.16', database='postgres',   user='junior', password='|?7LXmg+FWL&,2(')
    vendas = pd.read_sql('select * from public.venda', eng)
    vendas.rename(columns={'data_venda': 'data'},  inplace=True)
    vendas.to_sql('vendas', engine, index=False, if_exists='replace')
    #print(vendas)
    return vendas

def extract_func():
    list = []
    for i in range(1,10):
        url = f"https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={i}"
        try:
            user =  {'User-Agent': 'Chrome/100.0.4896.127'}
            response = requests.get(url, headers=user, timeout=30, verify=False)
        except requests.exceptions.RequestException:
            return None
        
        bs = BeautifulSoup(response.content,'html.parser') 
           
        list.append(bs)

    df = pd.DataFrame(list, columns=['funcionario'])
    #df = df.reset_index(names=['id'])
    #df.id = df.id + 1
    return df

def extract_category():
    file = pd.read_parquet('https://storage.googleapis.com/challenge_junior/categoria.parquet', engine='pyarrow')
    return file

def extract_data():
    data = extract_vendas()['data'];
    data = data.drop_duplicates()
    return data


def load_tables_dm():
    
    
    category = extract_category()['nome_categoria'];
    category.to_sql('dm_categoria', engine, index=False, if_exists='append')

    funcionario = extract_func();
    funcionario.to_sql('dm_funcionarios', engine, index=False, if_exists='append')

    data = extract_data();
    data.to_sql('dm_data', engine, index=False, if_exists='append')

    

def load_table_fato():
    venda = pd.read_sql_query('select v.venda, v.id_categoria, v.id_funcionario, d.id_data from vendas as v  inner join dm_data as d on v.data = d.data', engine)
    venda.to_sql('ft_vendas', engine, index=False, if_exists='append')


#if __name__ == "__main__":
#    load_table_fato()

    


    



