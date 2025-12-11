import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import numpy as np


st.set_page_config(page_title="Dashboard COVID-19 (Final)", layout="wide")

DB_USER = "postgres"
DB_PASS = "201005"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "projeto 3"

@st.cache_data(ttl=0)
def load_data():
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
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
        dd.cbo,
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
    

    df['data_notificacao'] = pd.to_datetime(df['data_notificacao'], errors='coerce').fillna(pd.Timestamp('2020-01-01'))
    
    def definir_status(x):
        x = str(x).lower()
        if 'confirmado' in x or 'laboratorial' in x: return 'Confirmado'
        if 'descartado' in x: return 'Descartado'
        if 'cura' in x: return 'Confirmado'
        if 'sindrome' in x or 'suspeito' in x: return 'Suspeito'
        return 'Em Análise'

    df['status'] = df['classificacao_final'].apply(definir_status)
    df['vacina_status'] = df['vacina_dose'].fillna(0).astype(int).astype(str) + " Doses"
    
    df['idade'] = pd.to_numeric(df['idade'], errors='coerce').fillna(0)
    df['sexo'] = df['sexo'].fillna('Indefinido')
    df['raca_cor'] = df['raca_cor'].fillna('Não Informado')
    
 
    df['cbo'] = df['cbo'].fillna('Não Informado').astype(str)
 
    df['cbo_curto'] = df['cbo'].apply(lambda x: x[:30] + '...' if len(x) > 30 else x)
    
    return df

@st.cache_data
def load_test_data():
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    query = "SELECT tipo_teste, fabricante_teste, resultado_teste FROM teste_laboratorial"
    df = pd.read_sql(query, engine)
    
    df['tipo_teste'] = df['tipo_teste'].fillna('Outros')
    
    df['fabricante_teste'] = df['fabricante_teste'].fillna('Não Informado').astype(str).str.upper()
 
    df['fabricante_curto'] = df['fabricante_teste'].apply(lambda x: x[:25] + '...' if len(x) > 25 else x)
    
    def check_positivo(res):
        res = str(res).lower()
        if 'reagente' in res and 'não' not in res: return 1
        if 'positivo' in res: return 1
        if 'detectavel' in res and 'não' not in res: return 1
        return 0
        
    df['is_positivo'] = df['resultado_teste'].apply(check_positivo)
    return df

@st.cache_data
def load_coordinates():
    url = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv"
    return pd.read_csv(url, usecols=['codigo_ibge', 'latitude', 'longitude'])

try:
    with st.spinner('Carregando dados...'):
        df = load_data()
        df_testes = load_test_data()
        df_geo = load_coordinates()
        df = df.merge(df_geo, left_on='municipio_notificacao_ibge', right_on='codigo_ibge', how='left')
except Exception as e:
    st.error(f"Erro: {e}")
    st.stop()


# FILTROS

st.sidebar.header("Filtros")
st.sidebar.success(f"Registros: {len(df):,}")

min_d = df['data_notificacao'].min()
max_d = df['data_notificacao'].max()
start_date, end_date = st.sidebar.date_input("Período", [min_d, max_d])

municipios = sorted([str(x) for x in df['municipio'].unique()])
cidade_selecionada = st.sidebar.multiselect("Municípios", municipios)

df_filtered = df[
    (df['data_notificacao'].dt.date >= start_date) & 
    (df['data_notificacao'].dt.date <= end_date)
]

if cidade_selecionada:
    df_filtered = df_filtered[df_filtered['municipio'].isin(cidade_selecionada)]


st.title("📊 Painel COVID-19 (PA)")

c1, c2, c3, c4 = st.columns(4)
total = len(df_filtered)
conf = len(df_filtered[df_filtered['status'] == 'Confirmado'])
obit = len(df_filtered[df_filtered['evolucao_caso'] == 'Obito'])
desc = len(df_filtered[df_filtered['status'] == 'Descartado'])

c1.metric("Total", f"{total:,}".replace(",", "."))
c2.metric("Confirmados", f"{conf:,}".replace(",", "."), delta_color="inverse")
c3.metric("Óbitos", f"{obit:,}".replace(",", "."), delta_color="inverse")
c4.metric("Descartados", f"{desc:,}".replace(",", "."))

st.markdown("---")


t1, t2, t3, t4 = st.tabs(["🌎 Mapa/Tempo", "👥 Perfil/Ocupação", "💉 Laboratório", "🤖 IA"])

# TEMPO E MAPA
with t1:
    col_map, col_line = st.columns([1, 1])
    with col_map:
        st.subheader("Mapa de Calor")
        if not df_filtered.empty and 'latitude' in df_filtered.columns:
            fig_map = px.density_mapbox(df_filtered.dropna(subset=['latitude']), 
                                        lat='latitude', lon='longitude', radius=13,
                                        center=dict(lat=-3.5, lon=-52), zoom=5,
                                        mapbox_style="carto-positron", height=500)
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.warning("Sem dados GPS.")

    with col_line:
        st.subheader("Linha do Tempo")
        df_chart = df_filtered.groupby([pd.Grouper(key='data_notificacao', freq='ME'), 'status']).size().reset_index(name='Casos')
        fig_line = px.line(df_chart, x='data_notificacao', y='Casos', color='status', markers=True, height=500)
        st.plotly_chart(fig_line, use_container_width=True)

# TAB 2: DEMOGRÁFICO E OCUPAÇÃO (CORRIGIDO)
with t2:
    r1c1, r1c2 = st.columns(2)
    with r1c1:
        st.subheader("Sexo")
        st.plotly_chart(px.pie(df_filtered, names='sexo', hole=0.4), use_container_width=True)
    with r1c2:
        st.subheader("Raça/Cor")
        st.plotly_chart(px.bar(df_filtered['raca_cor'].value_counts().reset_index(), x='raca_cor', y='count'), use_container_width=True)
    
    r2c1, r2c2 = st.columns(2)
    with r2c1:
        st.subheader("Idade")
        st.plotly_chart(px.histogram(df_filtered, x='idade', color='status', nbins=20), use_container_width=True)
    with r2c2:
        st.subheader("Top 10 Ocupações")
        # Usa a coluna truncada 'cbo_curto' para o gráfico não quebrar
        df_cbo = df_filtered[~df_filtered['cbo'].isin(['Não Informado', 'None', 'nan'])]
        if not df_cbo.empty:
            top_cbo = df_cbo['cbo_curto'].value_counts().head(10).reset_index()
            top_cbo.columns = ['Ocupação', 'Casos']
            st.plotly_chart(px.bar(top_cbo, y='Ocupação', x='Casos', orientation='h'), use_container_width=True)
        else:
            st.info("Sem dados de ocupação.")

# TAB 3: TESTES (CORRIGIDO ERRO NUMÉRICO)
with t3:
    c_vac, c_test = st.columns([1, 1])
    
    with c_vac:
        st.subheader("Vacinação")
        df_vac = pd.crosstab(df_filtered['vacina_status'], df_filtered['status'], normalize='index') * 100
        df_vac_long = df_vac.reset_index().melt(id_vars='vacina_status')
        st.plotly_chart(px.bar(df_vac_long, x='vacina_status', y='value', color='status'), use_container_width=True)

    with c_test:
        st.subheader("Laboratório")
        
        st.write("**Fabricantes (Top 10)**")
        if not df_testes.empty:
            
            df_fab = df_testes.groupby('fabricante_curto').agg(
                Total=('is_positivo', 'count'),
                Positivos=('is_positivo', 'sum')
            ).reset_index()
            

            df_fab['Total'] = pd.to_numeric(df_fab['Total'], errors='coerce').fillna(0)
            df_fab['Positivos'] = pd.to_numeric(df_fab['Positivos'], errors='coerce').fillna(0)
            
            
            df_fab['Taxa'] = 0.0
            mask = df_fab['Total'] > 0
            df_fab.loc[mask, 'Taxa'] = (df_fab.loc[mask, 'Positivos'] / df_fab.loc[mask, 'Total'] * 100)
            
            
            df_fab['Positividade (%)'] = df_fab['Taxa'].round(1)
            
            df_fab = df_fab.sort_values('Total', ascending=False).head(10)
            
            fig_combo = px.bar(df_fab, x='fabricante_curto', y='Total', 
                               color='Positividade (%)', 
                               title="Volume e Positividade")
            st.plotly_chart(fig_combo, use_container_width=True)
        else:
            st.warning("Sem dados.")


with t4:
    st.subheader("🤖 IA Preditiva")
    
    df_ml = df[['idade', 'sexo', 'status']].dropna().copy()
    df_ml = df_ml[df_ml['status'].isin(['Confirmado', 'Descartado'])]
    df_ml['target'] = df_ml['status'].apply(lambda x: 1 if x == 'Confirmado' else 0)
    
    if len(df_ml) > 100:
        le_sex = LabelEncoder()
        df_ml['sex_c'] = le_sex.fit_transform(df_ml['sexo'].astype(str))
        
        n_min = min(len(df_ml[df_ml['target']==1]), len(df_ml[df_ml['target']==0]))
        
        if n_min > 10:
            df_b = pd.concat([
                df_ml[df_ml['target']==1].sample(n_min, random_state=42),
                df_ml[df_ml['target']==0].sample(n_min, random_state=42)
            ])
            
            model = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42)
            model.fit(df_b[['idade', 'sex_c']], df_b['target'])
            
            with st.form("ia"):
                c1, c2 = st.columns(2)
                i = c1.number_input("Idade", 0, 120, 30)
                s = c2.selectbox("Sexo", df_ml['sexo'].unique())
                
                if st.form_submit_button("Calcular"):
                    try:
                        p = model.predict_proba([[i, le_sex.transform([s])[0]]])[0][1]
                        st.metric("Risco Positivo", f"{p*100:.1f}%")
                        st.progress(p)
                        if p > 0.5: st.error("Alto")
                        else: st.success("Baixo")
                    except: st.error("Erro.")
        else: st.warning("Balanceamento impossível.")
    else: st.warning("Dados insuficientes.")

st.markdown("---")
st.caption("Projeto Big Data")