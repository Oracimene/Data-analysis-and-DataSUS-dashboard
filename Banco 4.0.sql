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

--Etapa 3 pra baixo
CREATE TABLE IF NOT EXISTS log_alteracoes (
    log_id SERIAL PRIMARY KEY,
    tabela_afetada VARCHAR(50),
    operacao VARCHAR(10), 
    usuario VARCHAR(50) DEFAULT current_user,
    data_hora TIMESTAMP DEFAULT NOW(),
    dados_antigos JSONB, 
    dados_novos JSONB    
);

-- Função de Auditoria
CREATE OR REPLACE FUNCTION fn_auditoria_geral()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO log_alteracoes (tabela_afetada, operacao, dados_novos)
        VALUES (TG_TABLE_NAME, 'INSERT', row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_alteracoes (tabela_afetada, operacao, dados_antigos, dados_novos)
        VALUES (TG_TABLE_NAME, 'UPDATE', row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO log_alteracoes (tabela_afetada, operacao, dados_antigos)
        VALUES (TG_TABLE_NAME, 'DELETE', row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


-- Auditoria em Notificações
DROP TRIGGER IF EXISTS trg_auditoria_notificacao ON notificacao;
CREATE TRIGGER trg_auditoria_notificacao
AFTER INSERT OR UPDATE OR DELETE ON notificacao
FOR EACH ROW EXECUTE FUNCTION fn_auditoria_geral();

-- Auditoria em Testes Laboratoriais
DROP TRIGGER IF EXISTS trg_auditoria_testes ON teste_laboratorial;
CREATE TRIGGER trg_auditoria_testes
AFTER INSERT OR UPDATE OR DELETE ON teste_laboratorial
FOR EACH ROW EXECUTE FUNCTION fn_auditoria_geral();

CREATE TABLE IF NOT EXISTS indicadores_regionais (
    id SERIAL PRIMARY KEY,
    municipio_ibge INTEGER REFERENCES municipio(municipio_ibge),
    periodo_inicio DATE,
    periodo_fim DATE,
    total_testes INTEGER,
    total_positivos INTEGER,
    taxa_positividade DECIMAL(5,2),
    data_calculo TIMESTAMP DEFAULT NOW(),
    UNIQUE(municipio_ibge, periodo_inicio, periodo_fim)
);

-- Calcular Taxa de Positividade
CREATE OR REPLACE FUNCTION fx_calcular_taxa_positividade(p_inicio DATE, p_fim DATE)
RETURNS VOID AS $$
BEGIN
    INSERT INTO indicadores_regionais (municipio_ibge, periodo_inicio, periodo_fim, total_testes, total_positivos, taxa_positividade)
    SELECT 
        n.municipio_notificacao_ibge,
        p_inicio,
        p_fim,
        COUNT(*) as total_casos,
        SUM(CASE WHEN dc.classificacao_final ILIKE '%Confirmado%' OR dc.classificacao_final ILIKE '%Laboratorial%' THEN 1 ELSE 0 END) as total_positivos,
        CASE 
            WHEN COUNT(*) > 0 THEN 
                ROUND((SUM(CASE WHEN dc.classificacao_final ILIKE '%Confirmado%' OR dc.classificacao_final ILIKE '%Laboratorial%' THEN 1 ELSE 0 END)::DECIMAL / COUNT(*)) * 100, 2)
            ELSE 0 
        END as taxa
    FROM notificacao n
    JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
    WHERE n.data_notificacao BETWEEN p_inicio AND p_fim
      AND n.municipio_notificacao_ibge IS NOT NULL
    GROUP BY n.municipio_notificacao_ibge
    
    ON CONFLICT (municipio_ibge, periodo_inicio, periodo_fim) 
    DO UPDATE SET 
        total_testes = EXCLUDED.total_testes,
        total_positivos = EXCLUDED.total_positivos,
        taxa_positividade = EXCLUDED.taxa_positividade,
        data_calculo = NOW();
        
    RAISE NOTICE 'Indicadores calculados com sucesso para o período % a %', p_inicio, p_fim;
END;
$$ LANGUAGE plpgsql;

-- Tempo Médio de Sintomas até Notificação
CREATE OR REPLACE FUNCTION fx_tempo_medio_atendimento()
RETURNS TABLE (municipio VARCHAR, dias_medios INT) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.nome,
        CAST(AVG(n.data_notificacao - dc.data_inicio_sintomas) AS INT)
    FROM notificacao n
    JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
    JOIN municipio m ON n.municipio_notificacao_ibge = m.municipio_ibge
    WHERE dc.data_inicio_sintomas IS NOT NULL 
      AND n.data_notificacao >= dc.data_inicio_sintomas
    GROUP BY m.nome;
END;
$$ LANGUAGE plpgsql;

-- Casos por Município e Data
CREATE OR REPLACE VIEW vw_casos_por_municipio AS
SELECT 
    n.data_notificacao,
    m.nome as municipio_nome,
    m.municipio_ibge,
    COUNT(*) as total_notificacoes,
    SUM(CASE WHEN dc.classificacao_final ILIKE '%Confirmado%' THEN 1 ELSE 0 END) as confirmados,
    SUM(CASE WHEN dc.classificacao_final ILIKE '%Descartado%' THEN 1 ELSE 0 END) as descartados,
    SUM(CASE WHEN dc.classificacao_final ILIKE '%Suspeito%' OR dc.classificacao_final IS NULL THEN 1 ELSE 0 END) as suspeitos
FROM notificacao n
LEFT JOIN municipio m ON n.municipio_notificacao_ibge = m.municipio_ibge
LEFT JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
GROUP BY n.data_notificacao, m.nome, m.municipio_ibge;

-- Vacinação x Resultado (Cruza status vacinal com resultado clínico)
CREATE OR REPLACE VIEW vw_vacinacao_por_resultado AS
SELECT 
    COALESCE(v.max_dose, 0) || ' Doses' as status_vacinal,
    CASE 
        WHEN dc.classificacao_final ILIKE '%Confirmado%' THEN 'Positivo'
        WHEN dc.classificacao_final ILIKE '%Descartado%' THEN 'Negativo'
        ELSE 'Suspeito/Outros'
    END as resultado_teste,
    COUNT(*) as quantidade
FROM notificacao n
LEFT JOIN dados_clinicos dc ON n.notificacao_id = dc.notificacao_id
LEFT JOIN (
    SELECT notificacao_id, MAX(dose_numero) as max_dose 
    FROM vacina_aplicada 
    GROUP BY notificacao_id
) v ON n.notificacao_id = v.notificacao_id
GROUP BY 1, 2;

-- Sintomas Mais Frequentes 
CREATE OR REPLACE VIEW vw_sintomas_frequentes AS
SELECT 
    s.nome as sintoma,
    COUNT(*) as ocorrencias
FROM notificacao_sintoma ns
JOIN sintoma s ON ns.sintoma_id = s.sintoma_id
JOIN dados_clinicos dc ON ns.notificacao_id = dc.notificacao_id
WHERE dc.classificacao_final ILIKE '%Confirmado%'
GROUP BY s.nome
ORDER BY ocorrencias DESC;


--Testes pra ver c ta funcionando
SELECT fx_calcular_taxa_positividade('2020-01-01', '2025-12-31');
SELECT * FROM indicadores_regionais ORDER BY taxa_positividade DESC;

SELECT * FROM vw_sintomas_frequentes LIMIT 10;
SELECT * FROM vw_vacinacao_por_resultado;
SELECT * FROM vw_casos_por_municipio;

UPDATE notificacao SET validado = TRUE WHERE notificacao_id = 1;
SELECT * FROM log_alteracoes;

