import pandas as pd
import numpy as np
import re

# ============================================================
# 1. CARREGAMENTO
# ============================================================

df = pd.read_csv("DataSUS.csv", sep=",", dtype=str, low_memory=False)

# Remover espaços extras no nome das colunas
df.columns = df.columns.str.strip()

# ============================================================
# 2. PADRONIZAÇÃO DE VALORES GERAIS
# ============================================================

# Normalizar strings (substituindo applymap)
df = df.map(lambda x: x.lower().strip() if isinstance(x, str) else x)

# Converter strings vazias ou representações de nulo em NaN
df.replace(["", " ", "nan", "none", "null", "n/a", "na"], np.nan, inplace=True)

# ============================================================
# 3. CONVERSÃO DE DATAS
# ============================================================

date_cols = [
    "dataNotificacao", "dataInicioSintomas", "dataEncerramento",
    "dataPrimeiraDose", "dataSegundaDose",
    "dataColetaTeste1", "dataColetaTeste2",
    "dataColetaTeste3", "dataColetaTeste4"
]

for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors="coerce")

# ============================================================
# 4. PADRONIZAÇÃO DE CAMPOS SIMPLES
# ============================================================

### SEXO
df["sexo"] = df["sexo"].map({
    "m": "Masculino",
    "masculino": "Masculino",
    "f": "Feminino",
    "feminino": "Feminino"
})

### RAÇA/COR
map_raca = {
    "1": "Branca",
    "2": "Preta",
    "3": "Parda",
    "4": "Amarela",
    "5": "Indígena"
}
df["racaCor"] = df["racaCor"].map(map_raca).fillna(df["racaCor"])

# ============================================================
# 5. REMOÇÃO DE DUPLICATAS (ANTES DOS MULTIVALORADOS)
# ============================================================

df.drop_duplicates(inplace=True)

# Remover registros essenciais inválidos
df = df[df["dataNotificacao"].notna()]

# ============================================================
# 6. TRATAMENTO DE MULTIVALORADOS (AGORA PODE)
# ============================================================

def split_multivalued(x):
    """Divide valores usando vários separadores."""
    if isinstance(x, str):
        return [i.strip() for i in re.split(r"[;,/|]", x) if i.strip()]
    return np.nan

df["sintomas_list"] = df["sintomas"].apply(split_multivalued)
df["outrosSintomas_list"] = df["outrosSintomas"].apply(split_multivalued)
df["outrasCondicoes_list"] = df["outrasCondicoes"].apply(split_multivalued)
df["codigoDosesVacina_list"] = df["codigoDosesVacina"].apply(split_multivalued)

# ============================================================
# 7. CONVERSÃO DE CAMPOS NUMÉRICOS
# ============================================================

df["idade"] = pd.to_numeric(df["idade"], errors="coerce")
df["totalTestesRealizados"] = pd.to_numeric(df["totalTestesRealizados"], errors="coerce")

ibge_cols = [
    "estadoIBGE", "municipioIBGE",
    "municipioNotificacaoIBGE", "estadoNotificacaoIBGE"
]

for col in ibge_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ============================================================
# 8. RELATÓRIO DE INTEGRIDADE
# ============================================================

report = {}

report["percent_nulls"] = df.isna().mean().sort_values(ascending=False)
report["idade_outliers"] = df[(df["idade"] < 0) | (df["idade"] > 120)]["idade"]
report["total_registros_limpos"] = len(df)
report["sintomas_validos"] = df["sintomas_list"].notna().sum()

for key, value in report.items():
    print(f"\n===== {key.upper()} =====\n")
    print(value)

# ============================================================
# 9. SALVAR RESULTADO FINAL
# ============================================================

df.to_csv("DataSUS_tratado.csv", index=False)
print("\nArquivo final salvo como DataSUS_tratado.csv")
