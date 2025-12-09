DROP TABLE IF EXISTS notificacao_sintoma CASCADE;
DROP TABLE IF EXISTS notificacao_condicao CASCADE;
DROP TABLE IF EXISTS teste_laboratorial CASCADE;
DROP TABLE IF EXISTS vacina_aplicada CASCADE;
DROP TABLE IF EXISTS dados_demograficos CASCADE;
DROP TABLE IF EXISTS dados_clinicos CASCADE;
DROP TABLE IF EXISTS dados_epidemiologicos CASCADE;
DROP TABLE IF EXISTS dados_gestao_estrategia CASCADE;
DROP TABLE IF EXISTS sintoma CASCADE;
DROP TABLE IF EXISTS condicao CASCADE;
DROP TABLE IF EXISTS notificacao CASCADE;
DROP TABLE IF EXISTS municipio CASCADE;
DROP TABLE IF EXISTS estado CASCADE;
DROP TABLE IF EXISTS log_carga CASCADE;
CREATE TABLE IF NOT EXISTS estado (
    estado_ibge INTEGER PRIMARY KEY, 
    nome VARCHAR(100),
    sigla CHAR(2)
);

CREATE TABLE IF NOT EXISTS municipio (
    municipio_ibge INTEGER PRIMARY KEY,
    nome VARCHAR(150),
    estado_ibge INTEGER REFERENCES estado(estado_ibge)
);

-- 2. Tabela Central: Notificação
CREATE TABLE IF NOT EXISTS notificacao (
    notificacao_id BIGINT PRIMARY KEY, -- Usaremos o ID gerado pelo Python
    source_id VARCHAR(100),
    data_notificacao DATE,
    municipio_notificacao_ibge INTEGER REFERENCES municipio(municipio_ibge),
    estado_notificacao_ibge INTEGER REFERENCES estado(estado_ibge),
    excluido BOOLEAN DEFAULT FALSE,
    validado BOOLEAN DEFAULT FALSE
);

-- 3. Tabelas Satélites (1:1 com Notificação)

CREATE TABLE IF NOT EXISTS dados_demograficos (
    notificacao_id BIGINT PRIMARY KEY REFERENCES notificacao(notificacao_id),
    idade SMALLINT,
    sexo VARCHAR(20),
    raca_cor VARCHAR(50),
    is_profissional_saude VARCHAR(20),
    is_profissional_seguranca VARCHAR(20),
    cbo VARCHAR(200),
    pertence_comunidade_tradicional BOOLEAN
);

CREATE TABLE IF NOT EXISTS dados_clinicos (
    notificacao_id BIGINT PRIMARY KEY REFERENCES notificacao(notificacao_id),
    data_inicio_sintomas DATE,
    data_encerramento DATE,
    classificacao_final VARCHAR(150),
    evolucao_caso VARCHAR(150),
    outros_sintomas TEXT,
    outras_condicoes TEXT,
    total_testes_realizados INTEGER
);

CREATE TABLE IF NOT EXISTS dados_epidemiologicos (
    notificacao_id BIGINT PRIMARY KEY REFERENCES notificacao(notificacao_id),
    origem_dados VARCHAR(100),
    municipio_residencia_ibge INTEGER REFERENCES municipio(municipio_ibge),
    estado_residencia_ibge INTEGER REFERENCES estado(estado_ibge)
);

CREATE TABLE IF NOT EXISTS dados_gestao_estrategia (
    notificacao_id BIGINT PRIMARY KEY REFERENCES notificacao(notificacao_id),
    codigo_estrategia_covid VARCHAR(100),
    codigo_busca_ativa_assintomatico VARCHAR(100),
    outro_busca_ativa_assintomatico VARCHAR(255),
    codigo_triagem_populacao_especifica VARCHAR(100),
    outro_triagem_populacao_especifica VARCHAR(255),
    codigo_local_realizacao_testagem VARCHAR(100),
    outro_local_realizacao_testagem VARCHAR(255)
);

-- 4. Tabelas Relacionadas (1:N e N:N)

-- Sintomas (Normalizado)
CREATE TABLE IF NOT EXISTS sintoma (
    sintoma_id SERIAL PRIMARY KEY,
    nome VARCHAR(200) UNIQUE
);

CREATE TABLE IF NOT EXISTS notificacao_sintoma (
    notificacao_id BIGINT REFERENCES notificacao(notificacao_id),
    sintoma_id INTEGER REFERENCES sintoma(sintoma_id),
    PRIMARY KEY (notificacao_id, sintoma_id)
);

-- Condições (Normalizado)
CREATE TABLE IF NOT EXISTS condicao (
    condicao_id SERIAL PRIMARY KEY,
    nome VARCHAR(200) UNIQUE
);

CREATE TABLE IF NOT EXISTS notificacao_condicao (
    notificacao_id BIGINT REFERENCES notificacao(notificacao_id),
    condicao_id INTEGER REFERENCES condicao(condicao_id),
    PRIMARY KEY (notificacao_id, condicao_id)
);

-- Testes Laboratoriais (Até 4 por notificação)
CREATE TABLE IF NOT EXISTS teste_laboratorial (
    teste_id SERIAL PRIMARY KEY,
    notificacao_id BIGINT REFERENCES notificacao(notificacao_id),
    numero_sequencial SMALLINT,
    tipo_teste VARCHAR(150),
    fabricante_teste VARCHAR(255),
    resultado_teste VARCHAR(150),
    estado_teste VARCHAR(100),
    data_coleta DATE
);

-- Vacinação (Até 2 doses detalhadas)
CREATE TABLE IF NOT EXISTS vacina_aplicada (
    vacina_id SERIAL PRIMARY KEY,
    notificacao_id BIGINT REFERENCES notificacao(notificacao_id),
    dose_numero SMALLINT,
    data_aplicacao DATE,
    laboratorio VARCHAR(200),
    lote VARCHAR(100)
);

-- Log para controle
CREATE TABLE IF NOT EXISTS log_carga (
    id SERIAL PRIMARY KEY,
    data_execucao TIMESTAMP DEFAULT NOW(),
    registros_processados INTEGER,
    mensagem TEXT
);

select * from dados_gestao_estrategia



