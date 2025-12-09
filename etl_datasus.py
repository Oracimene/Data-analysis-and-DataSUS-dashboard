import pandas as pd
import numpy as np
import os
import re
from sqlalchemy import create_engine
import time
import unicodedata
from difflib import get_close_matches

# ============================================================
# CONFIGURAÇÕES
# ============================================================
DB_USER = "postgres"
DB_PASS = "201005"   
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "projeto 3"

CONN_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ARQUIVO_CSV = "DataSUS.csv"
PASTA_SAIDA = "csv_final"

os.makedirs(PASTA_SAIDA, exist_ok=True)

SINTOMAS_VALIDOS = [
    "Febre", "Tosse", "Dor De Garganta", "Dificuldade Respiratoria", 
    "Mialgia", "Dor No Corpo", "Diarreia", "Vomito", "Nausea",
    "Perda De Olfato", "Perda De Paladar", "Coriza", "Congestao Nasal",
    "Dor De Cabeca", "Cefaleia", "Fadiga", "Adinamia", "Fraqueza",
    "Dor Abdominal", "Dor Toracica", "Dor Lombar", "Dor Nos Olhos",
    "Calafrios", "Tontura", "Irritabilidade", "Sonolencia", "Mal Estar",
    "Assintomatico", "Cianose", "Hemoptise", "Espirros", "Producao De Catarro"
]

CONDICOES_VALIDAS = [
    "Diabetes", "Doenca Cardiovascular", "Hipertensao", "Doenca Respiratoria",
    "Obesidade", "Imunossupressao", "Doenca Renal", "Doenca Hepatica",
    "Doenca Neurologica", "Gestante", "Puerpera", "Sindrome De Down",
    "Neoplasia", "Cancer", "Asma", "Bronquite", "Tabagismo",
    "Doenca Hematologica", "Hipotireoidismo", "Idoso", "Profissional De Saude"
]

DE_PARA_FORCADO = {
    "agia": "Dor", "algia": "Dor", "algi": "Dor", "dores": "Dor",
    "adinafagia": "Dor De Garganta", "odinofagia": "Dor De Garganta",
    "ansia": "Nausea", "emese": "Vomito",
    "cabeca": "Dor De Cabeca", "enxaqueca": "Dor De Cabeca",
    "dispineia": "Dificuldade Respiratoria", "falta de ar": "Dificuldade Respiratoria", "cansaco": "Fadiga",
    "anosmia": "Perda De Olfato", "ageusia": "Perda De Paladar",
    "febril": "Febre", "temperatura": "Febre",
    "corpo": "Dor No Corpo", "juntas": "Dor No Corpo",
    "vertigem": "Tontura", "vertiegem": "Tontura",
    "disenteria": "Diarreia",
    "cardio": "Doenca Cardiovascular", "coracao": "Doenca Cardiovascular", "cardiaca": "Doenca Cardiovascular",
    "pulmao": "Doenca Respiratoria", "dpoc": "Doenca Respiratoria",
    "renal": "Doenca Renal", "rim": "Doenca Renal",
    "figado": "Doenca Hepatica",
    "imuno": "Imunossupressao",
    "gravida": "Gestante",
    "pressao": "Hipertensao", "has": "Hipertensao",
    "acidente": None, "trauma": None, "fratura": None, 
    "alzaimer": "Doenca Neurologica", "alzheimer": "Doenca Neurologica", "parkinson": "Doenca Neurologica",
    "alergia": None, "ansiedade": None, "depressao": None 
}

# ============================================================
# FUNÇÕES DE LIMPEZA E LÓGICA
# ============================================================

def normalizar_texto(texto):
    if not isinstance(texto, str): return ""
    texto = texto.lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def identificar_termo_canonico(texto_sujo, lista_valida):
    if not isinstance(texto_sujo, str): return None
    limpo = re.sub(r'^[\d\W_]+', '', texto_sujo).strip().replace(";", "").replace('"', '').replace("'", "")
    if len(limpo) < 3: return None 
    busca = normalizar_texto(limpo)
    
    termos_bloqueados = ['teste', 'exame', 'covid', 'positivo', 'negativo', 'reagente', 
                         'igg', 'igm', 'saturacao', 'transferencia', 'alta', 'obito', 
                         'isolamento', 'contato', 'monitoramento', 'acemp', 'alfis']
    if any(x in busca for x in termos_bloqueados): return None
    
    for chave, valor in DE_PARA_FORCADO.items():
        if chave in busca:
            if valor is None: return None
            if valor in lista_valida: return valor

    matches = get_close_matches(limpo.title(), lista_valida, n=1, cutoff=0.75)
    if matches: return matches[0]
        
    for valido in lista_valida:
        palavra_chave = normalizar_texto(valido.split()[0])
        if len(palavra_chave) > 3 and palavra_chave in busca:
            if palavra_chave not in ["doenca", "dor", "perda"]: return valido
            if "dor" in busca and "cabeca" in busca: return "Dor De Cabeca"
            if "dor" in busca and "garganta" in busca: return "Dor De Garganta"
            if "dor" in busca and "corpo" in busca: return "Dor No Corpo"
    return None

def processar_multivalorados(row, col_padrao, col_outros, lista_referencia):
    items = set()
    regex_split = r"[;,/|+]|\s+[eE]\s+|\s+-\s+"
    raw_text_list = []
    if col_padrao in row and pd.notna(row[col_padrao]): raw_text_list.append(str(row[col_padrao]))
    if col_outros in row and pd.notna(row[col_outros]): raw_text_list.append(str(row[col_outros]))

    for texto in raw_text_list:
        pedacos = re.split(regex_split, texto)
        for p in pedacos:
            termo_correto = identificar_termo_canonico(p, lista_referencia)
            if termo_correto: items.add(termo_correto)
    return list(items) if items else np.nan

def limpar_string(x):
    if isinstance(x, str):
        x = x.strip().replace(';', ',').replace('\n', ' ').replace('\r', '').replace('\\', '')
        if x in ["", "nan", "NaN", "null", "None", "undefined"]: return None
        return x
    return x

def limpar_datas(df, colunas):
    for col in colunas:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df

def limpar_inteiros(df, colunas):
    for col in colunas:
        if col in df.columns:
            series_num = pd.to_numeric(df[col], errors='coerce')
            df[col] = series_num.astype('Int64')
    return df

def inserir_via_copy(engine, nome_tabela, df, colunas_explicit=None):
    if df.empty: return
    arquivo_tmp = os.path.join(PASTA_SAIDA, f"{nome_tabela}_temp.csv")
    df.to_csv(arquivo_tmp, index=False, header=False, sep=';', na_rep='')
    try:
        with engine.connect() as conn:
            with conn.begin(): 
                cursor = conn.connection.cursor()
                with open(arquivo_tmp, 'r', encoding='utf-8') as f:
                    if colunas_explicit:
                        cursor.copy_from(f, nome_tabela, sep=';', null='', columns=colunas_explicit)
                    else:
                        cursor.copy_from(f, nome_tabela, sep=';', null='')
        print(f"  [OK] Inseridos {len(df)} registros em '{nome_tabela}'.")
    except Exception as e:
        print(f"  [ERRO] Falha em '{nome_tabela}': {e}")
    finally:
        if os.path.exists(arquivo_tmp): os.remove(arquivo_tmp)

# ============================================================
# MÓDULO DE RELATÓRIO DE INTEGRIDADE (NOVO!)
# ============================================================
def gerar_relatorio_estatistico(df_raw, outliers_idade_count, linhas_finais):
    print("\n--- Gerando Relatório de Integridade ---")
    
    total_linhas = len(df_raw)
    
    # 1. Percentual de Dados Faltantes (Nulos)
    nulos = df_raw.isnull().sum()
    nulos_pct = (nulos / total_linhas) * 100
    top_nulos = nulos_pct.sort_values(ascending=False).head(10)
    
    with open("relatorio_integridade.txt", "w", encoding="utf-8") as f:
        f.write("==================================================\n")
        f.write("RELATÓRIO ESTATÍSTICO DE INTEGRIDADE DE DADOS (ETL)\n")
        f.write("==================================================\n\n")
        
        f.write(f"DATA E HORA: {time.strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write(f"TOTAL DE REGISTROS PROCESSADOS: {total_linhas}\n\n")
        
        f.write("1. ANÁLISE DE DADOS FALTANTES (TOP 10 COLUNAS)\n")
        f.write("-" * 40 + "\n")
        for col, pct in top_nulos.items():
            f.write(f"{col:<30}: {pct:.2f}% nulos\n")
            
        f.write("\n2. OUTLIERS DETECTADOS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Idades Inválidas (<0 ou >120 anos): {outliers_idade_count} registros\n")
        # Exemplo: Datas futuras (assumindo que rodamos em 2025)
        if 'dataNotificacao' in df_raw.columns:
            datas_futuras = pd.to_datetime(df_raw['dataNotificacao'], errors='coerce')
            qtd_futuro = datas_futuras[datas_futuras > pd.Timestamp.now()].count()
            f.write(f"Datas de Notificação no Futuro: {qtd_futuro} registros\n")
            
        f.write("\n3. RESUMO DA NORMALIZAÇÃO\n")
        f.write("-" * 40 + "\n")
        f.write(f"Registros limpos inseridos no Banco: {linhas_finais}\n")
        f.write(f"Taxa de aproveitamento: {(linhas_finais/total_linhas)*100:.1f}%\n")
        
    print(f"  [OK] Relatório salvo em 'relatorio_integridade.txt'")

# ============================================================
# EXECUÇÃO DO ETL
# ============================================================
print("--- Iniciando Leitura do CSV ---")
start_time = time.time()

df = pd.read_csv(ARQUIVO_CSV, sep=",", dtype=str, low_memory=False)
df.columns = df.columns.str.strip()
df["id_gerado"] = range(1, len(df) + 1)

# --- CAPTURA DE ESTATÍSTICAS PARA O RELATÓRIO (ANTES DA LIMPEZA) ---
# Conta outliers de idade antes de limpar
if 'idade' in df.columns:
    idades_num = pd.to_numeric(df['idade'], errors='coerce')
    outliers_idade = idades_num[(idades_num < 0) | (idades_num > 120)].count()
else:
    outliers_idade = 0

print("--- Limpeza Básica ---")
df = df.map(limpar_string)
colunas_data = ["dataNotificacao", "dataInicioSintomas", "dataEncerramento", "dataPrimeiraDose", "dataSegundaDose"]
df = limpar_datas(df, colunas_data)
colunas_inteiras = ["idade", "totalTestesRealizados", "estadoNotificacaoIBGE", "municipioNotificacaoIBGE", "municipioIBGE", "estadoIBGE"]
df = limpar_inteiros(df, colunas_inteiras)
if 'idade' in df.columns: df.loc[(df["idade"] < 0) | (df["idade"] > 130), "idade"] = None

# Domínios
print("--- Processando Geografia ---")
df_estados = df[['estadoNotificacaoIBGE', 'estadoNotificacao']].dropna(subset=['estadoNotificacaoIBGE']).drop_duplicates(subset=['estadoNotificacaoIBGE'])
df_estados.columns = ['estado_ibge', 'nome']
df_estados['sigla'] = None 

df_mun = df[['municipioNotificacaoIBGE', 'municipioNotificacao', 'estadoNotificacaoIBGE']].dropna(subset=['municipioNotificacaoIBGE'])
df_mun = df_mun.drop_duplicates(subset=['municipioNotificacaoIBGE'])
df_mun.columns = ['municipio_ibge', 'nome', 'estado_ibge']
municipios_validos = set(df_mun['municipio_ibge'].dropna().unique())

# Notificacao e Satélites
df_notificacao = df[['id_gerado', 'source_id', 'dataNotificacao', 'municipioNotificacaoIBGE', 'estadoNotificacaoIBGE', 'excluido', 'validado']].copy()
map_bool = {'True': True, 'False': False, 'Sim': True, 'Não': False, '1': True, '0': False}
df_notificacao['excluido'] = df_notificacao['excluido'].map(map_bool).fillna(False)
df_notificacao['validado'] = df_notificacao['validado'].map(map_bool).fillna(False)
df_notificacao.columns = ['notificacao_id', 'source_id', 'data_notificacao', 'municipio_notificacao_ibge', 'estado_notificacao_ibge', 'excluido', 'validado']

df_demog = df[['id_gerado', 'idade', 'sexo', 'racaCor', 'profissionalSaude', 'profissionalSeguranca', 'cbo', 'codigoContemComunidadeTradicional']].copy()
df_demog['codigoContemComunidadeTradicional'] = df_demog['codigoContemComunidadeTradicional'].map({'1': True, '0': False}).fillna(False)
df_demog.columns = ['notificacao_id', 'idade', 'sexo', 'raca_cor', 'is_profissional_saude', 'is_profissional_seguranca', 'cbo', 'pertence_comunidade_tradicional']

df_clin = df[['id_gerado', 'dataInicioSintomas', 'dataEncerramento', 'classificacaoFinal', 'evolucaoCaso', 'outrosSintomas', 'outrasCondicoes', 'totalTestesRealizados']].copy()
for c in ['classificacaoFinal', 'evolucaoCaso', 'outrosSintomas', 'outrasCondicoes']:
    if c in df_clin.columns: df_clin[c] = df_clin[c].astype(str).str.replace(';', ',').str.slice(0, 149)
df_clin.columns = ['notificacao_id', 'data_inicio_sintomas', 'data_encerramento', 'classificacao_final', 'evolucao_caso', 'outros_sintomas', 'outras_condicoes', 'total_testes_realizados']

df_epidem = df[['id_gerado', 'origem', 'municipioIBGE', 'estadoIBGE']].copy()
df_epidem.columns = ['notificacao_id', 'origem_dados', 'municipio_residencia_ibge', 'estado_residencia_ibge']
df_epidem = df_epidem[df_epidem['municipio_residencia_ibge'].isin(municipios_validos)]

cols_gestao = ['id_gerado', 'codigoEstrategiaCovid', 'codigoBuscaAtivaAssintomatico', 'outroBuscaAtivaAssintomatico', 'codigoTriagemPopulacaoEspecifica', 'outroTriagemPopulacaoEspecifica', 'codigoLocalRealizacaoTestagem', 'outroLocalRealizacaoTestagem']
df_gestao = df[[c for c in cols_gestao if c in df.columns]].copy()
col_names = ['notificacao_id', 'codigo_estrategia_covid', 'codigo_busca_ativa_assintomatico', 'outro_busca_ativa_assintomatico', 'codigo_triagem_populacao_especifica', 'outro_triagem_populacao_especifica', 'codigo_local_realizacao_testagem', 'outro_local_realizacao_testagem']
df_gestao.columns = col_names[:len(df_gestao.columns)]

# ============================================================
# SINTOMAS E CONDIÇÕES
# ============================================================
print("--- Processando Sintomas e Condições (Fuzzy + Whitelist) ---")

df["lista_final_sintomas"] = df.apply(lambda x: processar_multivalorados(x, "sintomas", "outrosSintomas", SINTOMAS_VALIDOS), axis=1)
col_cond = "condicoes" if "condicoes" in df.columns else "comorbidades"
df["lista_final_condicoes"] = df.apply(lambda x: processar_multivalorados(x, col_cond, "outrasCondicoes", CONDICOES_VALIDAS), axis=1)

def preparar_tabelas_dim(df_source, col_lista):
    df_temp = df_source[['id_gerado', col_lista]].explode(col_lista)
    df_temp = df_temp.dropna(subset=[col_lista])
    
    dim = df_temp[[col_lista]].drop_duplicates().sort_values(by=col_lista).reset_index(drop=True)
    dim['id'] = dim.index + 1
    dim.columns = ['nome', 'id']
    
    bridge = df_temp.merge(dim, left_on=col_lista, right_on='nome', how='inner')
    bridge = bridge[['id_gerado', 'id']]
    return dim, bridge

df_sintomas_dim, df_notificacao_sintoma = preparar_tabelas_dim(df, "lista_final_sintomas")
df_condicoes_dim, df_notificacao_condicao = preparar_tabelas_dim(df, "lista_final_condicoes")

# Carga
print("\n--- INICIANDO CARGA ---")
try:
    engine = create_engine(CONN_STRING)
    
    inserir_via_copy(engine, 'estado', df_estados)
    inserir_via_copy(engine, 'municipio', df_mun)
    inserir_via_copy(engine, 'notificacao', df_notificacao)
    
    inserir_via_copy(engine, 'dados_demograficos', df_demog)
    inserir_via_copy(engine, 'dados_clinicos', df_clin)
    inserir_via_copy(engine, 'dados_epidemiologicos', df_epidem)
    inserir_via_copy(engine, 'dados_gestao_estrategia', df_gestao)
    
    if not df_sintomas_dim.empty:
        df_sintomas_dim = df_sintomas_dim[['id', 'nome']]
        inserir_via_copy(engine, 'sintoma', df_sintomas_dim, colunas_explicit=['sintoma_id', 'nome'])
        df_notificacao_sintoma.columns = ['notificacao_id', 'sintoma_id']
        inserir_via_copy(engine, 'notificacao_sintoma', df_notificacao_sintoma)
        
    if not df_condicoes_dim.empty:
        df_condicoes_dim = df_condicoes_dim[['id', 'nome']]
        inserir_via_copy(engine, 'condicao', df_condicoes_dim, colunas_explicit=['condicao_id', 'nome'])
        df_notificacao_condicao.columns = ['notificacao_id', 'condicao_id']
        inserir_via_copy(engine, 'notificacao_condicao', df_notificacao_condicao)
        
    # GERA O RELATÓRIO AO FINAL DO SUCESSO
    gerar_relatorio_estatistico(df, outliers_idade, len(df_notificacao))

    print("\n--- SUCESSO! ---")
    print(f"Tempo: {round(time.time() - start_time, 2)}s")

except Exception as e:
    print(f"\nERRO: {e}")

