import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import numpy as np

# ============================================================
# 1. CONFIGURAÇÃO E CONEXÃO
# ============================================================
st.set_page_config(page_title="Dashboard Epidemiológico COVID-19", layout="wide")

DB_USER = "postgres"
DB_PASS = "201005"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "projeto 3"

@st.cache_data(ttl=0)
def load_data():
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # QUERY BLINDADA (LEFT JOIN)
    query = """
    SELECT 
        n.notificacao_id,
        n.data_notificacao,
        COALESCE(m.nome, 'Município Desconhecido') as municipio,
        n.municipio_notificacao_ibge,
        dc.classificacao_final,
        dc.evolucao_caso,
        dd.idade,
        dd.sexo,
        dd.raca_cor,
        dd.is_profissional_saude,
        v.dose_numero as vacina_dose
    FROM notificacao n
    LEFT JOIN municipio m ON n.municipio_notificacao_ibge = m.municipio_ibge
    LEFT JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
    LEFT JOIN dados_demograficos dd ON n.notificacao_id = dd.notificacao_id
    LEFT JOIN (
        SELECT notificacao_id, MAX(dose_numero) as dose_numero 
        FROM vacina_aplicada 
        GROUP BY notificacao_id
    ) v ON n.notificacao_id = v.notificacao_id
    """
    
    df = pd.read_sql(query, engine)
    
    # --- O PULO DO GATO (RECUPERAÇÃO DOS 91K) ---
    # 1. Converte para datetime
    df['data_notificacao'] = pd.to_datetime(df['data_notificacao'], errors='coerce')
    
    # 2. IMPORTANTE: Preenche datas vazias com 01/01/2020
    # Se não fizer isso, o filtro de data do Streamlit esconde essas linhas!
    df['data_notificacao'] = df['data_notificacao'].fillna(pd.Timestamp('2020-01-01'))
    
    # 3. Tratamento de Status
    def definir_status(x):
        x = str(x).lower()
        if 'confirmado' in x or 'laboratorial' in x: return 'Confirmado'
        if 'descartado' in x: return 'Descartado'
        if 'cura' in x: return 'Confirmado'
        if 'sindrome' in x or 'suspeito' in x: return 'Suspeito'
        return 'Em Análise'

    df['status'] = df['classificacao_final'].apply(definir_status)
    df['vacina_status'] = df['vacina_dose'].fillna(0).astype(int).astype(str) + " Doses"
    
    # 4. Limpeza Visual
    df['idade'] = df['idade'].fillna(0)
    df['sexo'] = df['sexo'].fillna('Indefinido')
    df['raca_cor'] = df['raca_cor'].fillna('Não Informado')
    
    return df

@st.cache_data
def load_test_data():
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    return pd.read_sql("SELECT tipo_teste FROM teste_laboratorial", engine)

@st.cache_data
def load_coordinates():
    url = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv"
    return pd.read_csv(url, usecols=['codigo_ibge', 'latitude', 'longitude'])

try:
    with st.spinner('Carregando e processando 100% dos dados...'):
        df = load_data()
        df_geo = load_coordinates()
        df_testes = load_test_data()
        
        # Merge Geo
        df = df.merge(df_geo, left_on='municipio_notificacao_ibge', right_on='codigo_ibge', how='left')
        
except Exception as e:
    st.error(f"Erro: {e}")
    st.stop()

# ============================================================
# 2. BARRA LATERAL (FILTROS)
# ============================================================
st.sidebar.header("Filtros")

# Debug: Mostra o total real carregado
st.sidebar.info(f"Dados Carregados: {len(df):,}")

# Filtro de Data
min_date = df['data_notificacao'].min()
max_date = df['data_notificacao'].max()
start_date, end_date = st.sidebar.date_input("Período", [min_date, max_date])

# Filtro de Município
municipios = sorted([str(x) for x in df['municipio'].unique()])
cidade_selecionada = st.sidebar.multiselect("Selecione Municípios", municipios)

# Aplicação dos Filtros
df_filtered = df[
    (df['data_notificacao'].dt.date >= start_date) & 
    (df['data_notificacao'].dt.date <= end_date)
]

if cidade_selecionada:
    df_filtered = df_filtered[df_filtered['municipio'].isin(cidade_selecionada)]

# ============================================================
# 3. KPI'S (LAYOUT ANTIGO)
# ============================================================
st.title("📊 Monitoramento COVID-19 - Pará")

col1, col2, col3, col4 = st.columns(4)

total_casos = len(df_filtered)
confirmados = len(df_filtered[df_filtered['status'] == 'Confirmado'])
obitos = len(df_filtered[df_filtered['evolucao_caso'] == 'Obito'])
# Mostra Descartados em vez de taxa para dar noção do volume total
descartados = len(df_filtered[df_filtered['status'] == 'Descartado'])

col1.metric("Notificações Totais", f"{total_casos:,}".replace(",", "."))
col2.metric("Casos Confirmados", f"{confirmados:,}".replace(",", "."), delta_color="inverse")
col3.metric("Óbitos Confirmados", f"{obitos:,}".replace(",", "."), delta_color="inverse")
col4.metric("Casos Descartados", f"{descartados:,}".replace(",", "."))

st.markdown("---")

# ============================================================
# 4. TABS DO DASHBOARD
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs(["🌎 Visão Temporal e Geográfica", "👥 Perfil Demográfico", "💉 Testes e Vacinas", "🤖 Predição (IA)"])

# --- TAB 1: TEMPORAL E MAPAS ---
with tab1:
    st.subheader("Evolução Temporal")
    st.caption("*Nota: O pico em Jan/2020 representa notificações que não possuíam data e foram recuperadas.*")
    
    df_time = df_filtered.groupby([pd.Grouper(key='data_notificacao', freq='ME'), 'status']).size().reset_index(name='count')
    
    fig_time = px.line(df_time, x='data_notificacao', y='count', color='status', 
                       title="Evolução de Casos", markers=True)
    st.plotly_chart(fig_time, use_container_width=True)

    st.subheader("Mapa de Calor de Notificações")
    if not df_filtered.empty and 'latitude' in df_filtered.columns:
        fig_map = px.density_mapbox(df_filtered.dropna(subset=['latitude']), 
                                    lat='latitude', lon='longitude',
                                    radius=15, center=dict(lat=-3.5, lon=-52), zoom=5,
                                    mapbox_style="carto-positron",
                                    height=600)
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("Dados geográficos indisponíveis.")

# --- TAB 2: DEMOGRÁFICO ---
with tab2:
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Distribuição por Sexo")
        fig_sex = px.pie(df_filtered, names='sexo', hole=0.4)
        st.plotly_chart(fig_sex, use_container_width=True)
        
    with col_b:
        st.subheader("Distribuição por Raça/Cor")
        fig_raca = px.bar(df_filtered['raca_cor'].value_counts().reset_index(), 
                          x='raca_cor', y='count', title="Casos por Raça/Cor")
        st.plotly_chart(fig_raca, use_container_width=True)
        
    st.subheader("Distribuição por Faixa Etária")
    fig_age = px.histogram(df_filtered, x="idade", nbins=20, color="status", 
                           title="Histograma de Idade", barmode="overlay")
    st.plotly_chart(fig_age, use_container_width=True)

# --- TAB 3: TESTES E VACINAS ---
with tab3:
    col_c, col_d = st.columns(2)
    
    with col_c:
        st.subheader("Vacinação vs Confirmação")
        df_vac_conf = pd.crosstab(df_filtered['vacina_status'], df_filtered['status'], normalize='index') * 100
        df_vac_long = df_vac_conf.reset_index().melt(id_vars='vacina_status', var_name='Status', value_name='Percentual')
        
        fig_vac = px.bar(df_vac_long, x='vacina_status', y='Percentual', color='Status', 
                         title="Impacto da Vacinação (%)")
        st.plotly_chart(fig_vac, use_container_width=True)

    with col_d:
        st.subheader("Tipos de Testes Realizados")
        top_testes = df_testes['tipo_teste'].value_counts().head(10).reset_index()
        fig_test = px.bar(top_testes, x='count', y='tipo_teste', orientation='h', 
                          title="Top Testes Utilizados")
        st.plotly_chart(fig_test, use_container_width=True)

# --- TAB 4: MODELO PREDITIVO (IA) ---
with tab4:
    st.subheader("🤖 Calculadora de Risco (Probabilidade de Positivo)")
    st.markdown("Modelo *Random Forest* treinado com os dados atuais do banco.")

    # Dados para ML
    df_ml = df[['idade', 'sexo', 'vacina_status', 'status']].dropna().copy()
    df_ml['target'] = df_ml['status'].apply(lambda x: 1 if x == 'Confirmado' else 0)
    
    if len(df_ml) > 100:
        # Encoding
        le_sexo = LabelEncoder()
        df_ml['sexo_code'] = le_sexo.fit_transform(df_ml['sexo'].astype(str))
        
        le_vacina = LabelEncoder()
        df_ml['vacina_code'] = le_vacina.fit_transform(df_ml['vacina_status'].astype(str))
        
        # Treino
        X = df_ml[['idade', 'sexo_code', 'vacina_code']]
        y = df_ml['target']
        
        model = RandomForestClassifier(n_estimators=30, max_depth=7)
        model.fit(X, y)
        
        with st.form("form_predicao"):
            col_in1, col_in2, col_in3 = st.columns(3)
            idade_input = col_in1.number_input("Idade", 0, 120, 30)
            sexo_input = col_in2.selectbox("Sexo", df_ml['sexo'].unique())
            vacina_input = col_in3.selectbox("Status Vacinal", df_ml['vacina_status'].unique())
            
            if st.form_submit_button("Calcular Risco"):
                # Input
                s_code = le_sexo.transform([str(sexo_input)])[0]
                v_code = le_vacina.transform([str(vacina_input)])[0]
                entrada = np.array([[idade_input, s_code, v_code]])
                
                # Predição
                proba = model.predict_proba(entrada)[0][1]
                
                st.metric("Probabilidade de COVID-19", f"{proba*100:.1f}%")
                if proba > 0.5:
                    st.error("Risco Elevado.")
                else:
                    st.success("Risco Baixo.")
    else:
        st.warning("Dados insuficientes para treinar o modelo.")

st.markdown("---")
st.caption("Dashboard de Saúde | Fonte: DataSUS")