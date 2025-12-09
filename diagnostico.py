import pandas as pd
from sqlalchemy import create_engine

# Configuração
DB_USER = "postgres"
DB_PASS = "201005"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "projeto 3"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

print("--- INICIANDO DIAGNÓSTICO DO BANCO DE DADOS ---")

# 1. Contagem Bruta
qtd_raw = pd.read_sql("SELECT count(*) FROM notificacao", engine).iloc[0,0]
print(f"1. Total na tabela 'notificacao': {qtd_raw} (Esperado: ~148k)")

# 2. Verificando Datas Nulas (O Grande Vilão)
qtd_data_nula = pd.read_sql("SELECT count(*) FROM notificacao WHERE data_notificacao IS NULL", engine).iloc[0,0]
print(f"2. Linhas com Data Nula: {qtd_data_nula}")

# 3. Verificando Join com Município
query_join = """
SELECT count(*) 
FROM notificacao n 
JOIN municipio m ON n.municipio_notificacao_ibge = m.municipio_ibge
"""
qtd_join = pd.read_sql(query_join, engine).iloc[0,0]
print(f"3. Total após JOIN com Município (INNER JOIN): {qtd_join}")

# 4. Verificando Limites de Datas
datas = pd.read_sql("SELECT min(data_notificacao), max(data_notificacao) FROM notificacao", engine)
print(f"4. Período dos dados: De {datas.iloc[0,0]} até {datas.iloc[0,1]}")

print("----------------------------------------------")
if qtd_raw == 148741 and qtd_join < 60000:
    print("CONCLUSÃO: O problema é o MUNICÍPIO. Muitos códigos IBGE da notificação não existem na tabela de municípios.")
    print("SOLUÇÃO: O 'LEFT JOIN' no app.py é obrigatório.")

elif qtd_raw == 148741 and qtd_data_nula > 50000:
    print("CONCLUSÃO: O problema são DATAS NULAS. O Pandas/Streamlit filtra datas vazias.")
    print("SOLUÇÃO: Preencher datas vazias no dashboard.py.")

elif qtd_raw < 60000:
    print("CONCLUSÃO: O problema é no ETL. Os dados NÃO entraram no banco.")
    print("SOLUÇÃO: Rodar o ETL novamente ou verificar o CSV.")
else:
    print("CONCLUSÃO: O banco está correto. O problema é CACHE do Streamlit.")