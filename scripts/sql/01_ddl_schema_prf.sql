-- =============================================================================
-- PROJETO: Normalização de Dados Abertos da PRF (2023–2026)
-- SGBD: PostgreSQL (execução via DBeaver)
-- ETAPA: 01 - Criação dos Schemas, Tabela de Staging e Modelo Relacional 3FN
-- =============================================================================
-- Critérios atendidos:
--   * Ambiente relacional normalizado em 3FN (schema "prf"): 18 tabelas (>= 10)
--   * Tabelas principais com >= 10.000 linhas (268 mil a 659 mil linhas reais)
--   * Cobertura temporal dos últimos 3+ anos (2023-01-01 a 2026-09-30)
--   * Coluna atualizado_em preparada para futura carga incremental (Spark/Airflow)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS prf;

-- =============================================================================
-- 1. TABELA DE STAGING (Recebe os 4 CSVs brutos da PRF sem erro de tipagem)
-- =============================================================================
DROP TABLE IF EXISTS staging.acidentes_raw;

CREATE TABLE staging.acidentes_raw (
    id                      TEXT,
    pesid                   TEXT,
    data_inversa            TEXT,
    dia_semana              TEXT,
    horario                 TEXT,
    uf                      TEXT,
    br                      TEXT,
    km                      TEXT,
    municipio               TEXT,
    causa_acidente          TEXT,
    tipo_acidente           TEXT,
    classificacao_acidente  TEXT,
    fase_dia                TEXT,
    sentido_via             TEXT,
    condicao_metereologica  TEXT, -- Mantido nome original do cabeçalho do CSV da PRF
    tipo_pista              TEXT,
    tracado_via             TEXT,
    uso_solo                TEXT,
    id_veiculo              TEXT,
    tipo_veiculo            TEXT,
    marca                   TEXT,
    ano_fabricacao_veiculo  TEXT,
    tipo_envolvido          TEXT,
    estado_fisico           TEXT,
    idade                   TEXT,
    sexo                    TEXT,
    ilesos                  TEXT,
    feridos_leves           TEXT,
    feridos_graves          TEXT,
    mortos                  TEXT,
    latitude                TEXT,
    longitude               TEXT,
    regional                TEXT,
    delegacia               TEXT,
    uop                     TEXT
);

COMMENT ON TABLE staging.acidentes_raw IS
    'Área temporária de ingestão dos CSVs brutos da PRF (encoding ISO-8859-1/WIN1252, delimitador ";").';

-- =============================================================================
-- 2. LIMPEZA SEGURA DO SCHEMA NORMALIZADO (Para reexecução limpa, se necessário)
-- =============================================================================
DROP TABLE IF EXISTS prf.envolvido CASCADE;
DROP TABLE IF EXISTS prf.veiculo CASCADE;
DROP TABLE IF EXISTS prf.acidente_tracado_via CASCADE;
DROP TABLE IF EXISTS prf.acidente CASCADE;
DROP TABLE IF EXISTS prf.modelo_veiculo CASCADE;
DROP TABLE IF EXISTS prf.fabricante_veiculo CASCADE;
DROP TABLE IF EXISTS prf.tipo_veiculo CASCADE;
DROP TABLE IF EXISTS prf.tipo_envolvido CASCADE;
DROP TABLE IF EXISTS prf.estado_fisico CASCADE;
DROP TABLE IF EXISTS prf.tracado_via CASCADE;
DROP TABLE IF EXISTS prf.condicao_meteorologica CASCADE;
DROP TABLE IF EXISTS prf.tipo_acidente CASCADE;
DROP TABLE IF EXISTS prf.causa_acidente CASCADE;
DROP TABLE IF EXISTS prf.uop_prf CASCADE;
DROP TABLE IF EXISTS prf.delegacia_prf CASCADE;
DROP TABLE IF EXISTS prf.regional_prf CASCADE;
DROP TABLE IF EXISTS prf.municipio CASCADE;
DROP TABLE IF EXISTS prf.uf CASCADE;

-- =============================================================================
-- 3. TABELAS DE DOMÍNIO E HIERARQUIA (Resolução de 3FN — 14 tabelas)
-- =============================================================================

-- 3.1 Geografia: UF -> Município
CREATE TABLE prf.uf (
    sigla_uf CHAR(2) PRIMARY KEY
);
COMMENT ON TABLE prf.uf IS 'Unidades Federativas do Brasil (27 UFs).';

CREATE TABLE prf.municipio (
    id_municipio    SERIAL PRIMARY KEY,
    sigla_uf        CHAR(2) NOT NULL REFERENCES prf.uf(sigla_uf),
    nome_municipio  VARCHAR(120) NOT NULL,
    CONSTRAINT uq_municipio_uf_nome UNIQUE (sigla_uf, nome_municipio)
);
COMMENT ON TABLE prf.municipio IS 'Municípios normalizados em 3FN, eliminando dependência transitiva acidente -> município -> UF.';

-- 3.2 Hierarquia Organizacional da PRF: Regional -> Delegacia -> UOP
CREATE TABLE prf.regional_prf (
    id_regional      SERIAL PRIMARY KEY,
    codigo_regional  VARCHAR(40) NOT NULL UNIQUE
);
COMMENT ON TABLE prf.regional_prf IS 'Superintendências Regionais da PRF (ex.: SPRF-SC, SPRF-SP).';

CREATE TABLE prf.delegacia_prf (
    id_delegacia      SERIAL PRIMARY KEY,
    codigo_delegacia  VARCHAR(40) NOT NULL UNIQUE,
    id_regional       INTEGER NOT NULL REFERENCES prf.regional_prf(id_regional)
);
COMMENT ON TABLE prf.delegacia_prf IS 'Delegacias da PRF vinculadas à respectiva Regional (3FN).';

CREATE TABLE prf.uop_prf (
    id_uop        SERIAL PRIMARY KEY,
    codigo_uop    VARCHAR(50) NOT NULL UNIQUE,
    id_delegacia  INTEGER NOT NULL REFERENCES prf.delegacia_prf(id_delegacia)
);
COMMENT ON TABLE prf.uop_prf IS 'Unidades Operacionais da PRF vinculadas à respectiva Delegacia (3FN).';

-- 3.3 Domínios da Ocorrência
CREATE TABLE prf.causa_acidente (
    id_causa_acidente  SERIAL PRIMARY KEY,
    descricao          VARCHAR(200) NOT NULL UNIQUE
);

CREATE TABLE prf.tipo_acidente (
    id_tipo_acidente  SERIAL PRIMARY KEY,
    descricao         VARCHAR(150) NOT NULL UNIQUE
);

CREATE TABLE prf.condicao_meteorologica (
    id_condicao_meteorologica  SERIAL PRIMARY KEY,
    descricao                  VARCHAR(80) NOT NULL UNIQUE
);

CREATE TABLE prf.tracado_via (
    id_tracado_via  SERIAL PRIMARY KEY,
    descricao       VARCHAR(80) NOT NULL UNIQUE
);
COMMENT ON TABLE prf.tracado_via IS 'Valores atômicos de traçado de via (resolve violação de 1FN do campo multivalorado separado por ";").';

-- 3.4 Domínios de Veículo (3FN: Veículo -> Modelo -> Fabricante)
CREATE TABLE prf.tipo_veiculo (
    id_tipo_veiculo  SERIAL PRIMARY KEY,
    descricao        VARCHAR(80) NOT NULL UNIQUE
);

CREATE TABLE prf.fabricante_veiculo (
    id_fabricante    SERIAL PRIMARY KEY,
    nome_fabricante  VARCHAR(120) NOT NULL UNIQUE
);
COMMENT ON TABLE prf.fabricante_veiculo IS 'Fabricante extraído antes da barra "/" na coluna marca.';

CREATE TABLE prf.modelo_veiculo (
    id_modelo_veiculo  SERIAL PRIMARY KEY,
    id_fabricante      INTEGER NOT NULL REFERENCES prf.fabricante_veiculo(id_fabricante),
    nome_modelo        VARCHAR(180) NOT NULL,
    CONSTRAINT uq_modelo_fabricante UNIQUE (id_fabricante, nome_modelo)
);
COMMENT ON TABLE prf.modelo_veiculo IS 'Modelo de veículo vinculado ao fabricante (3FN, ~11.192 registros).';

-- 3.5 Domínios de Envolvido (Pessoa)
CREATE TABLE prf.tipo_envolvido (
    id_tipo_envolvido  SERIAL PRIMARY KEY,
    descricao          VARCHAR(60) NOT NULL UNIQUE
);

CREATE TABLE prf.estado_fisico (
    id_estado_fisico  SERIAL PRIMARY KEY,
    descricao         VARCHAR(60) NOT NULL UNIQUE
);
COMMENT ON TABLE prf.estado_fisico IS 'Condição física do envolvido. Elimina colunas derivadas ilesos/feridos_leves/feridos_graves/mortos (3FN).';

-- =============================================================================
-- 4. TABELAS PRINCIPAIS E ASSOCIATIVA (1FN e 2FN — 4 tabelas, todas > 268k linhas)
-- =============================================================================

-- 4.1 Tabela Principal: Acidente (1 linha por ocorrência — ~268.392 linhas)
CREATE TABLE prf.acidente (
    id_acidente                BIGINT PRIMARY KEY,
    data_acidente              DATE NOT NULL,
    horario                    TIME NOT NULL,
    id_municipio               INTEGER NOT NULL REFERENCES prf.municipio(id_municipio),
    br                         SMALLINT NULL,
    km                         NUMERIC(6, 1) NULL,
    latitude                   NUMERIC(11, 8) NULL,
    longitude                  NUMERIC(11, 8) NULL,
    id_uop                     INTEGER NULL REFERENCES prf.uop_prf(id_uop),
    id_causa_acidente          INTEGER NOT NULL REFERENCES prf.causa_acidente(id_causa_acidente),
    id_tipo_acidente           INTEGER NOT NULL REFERENCES prf.tipo_acidente(id_tipo_acidente),
    id_condicao_meteorologica  INTEGER NOT NULL REFERENCES prf.condicao_meteorologica(id_condicao_meteorologica),
    classificacao_acidente     VARCHAR(40) NULL,
    fase_dia                   VARCHAR(30) NOT NULL,
    sentido_via                VARCHAR(30) NOT NULL,
    tipo_pista                 VARCHAR(30) NOT NULL,
    uso_solo_urbano            BOOLEAN NOT NULL,
    atualizado_em              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE prf.acidente IS 'Tabela principal de ocorrências de acidentes na malha rodoviária federal (1 linha por id).';

-- 4.2 Tabela Associativa N:N (1FN): Acidente <-> Traçado da Via (~344.682 linhas)
CREATE TABLE prf.acidente_tracado_via (
    id_acidente     BIGINT NOT NULL REFERENCES prf.acidente(id_acidente) ON DELETE CASCADE,
    id_tracado_via  INTEGER NOT NULL REFERENCES prf.tracado_via(id_tracado_via),
    PRIMARY KEY (id_acidente, id_tracado_via)
);
COMMENT ON TABLE prf.acidente_tracado_via IS 'Relacionamento N:N que atomiciza a coluna multivalorada tracado_via (1FN).';

-- 4.3 Tabela Principal: Veículo (1 linha por veículo envolvido — ~515.106 linhas)
CREATE TABLE prf.veiculo (
    id_veiculo         BIGINT PRIMARY KEY,
    id_acidente        BIGINT NOT NULL REFERENCES prf.acidente(id_acidente) ON DELETE CASCADE,
    id_tipo_veiculo    INTEGER NULL REFERENCES prf.tipo_veiculo(id_tipo_veiculo),
    id_modelo_veiculo  INTEGER NOT NULL REFERENCES prf.modelo_veiculo(id_modelo_veiculo),
    ano_fabricacao     SMALLINT NULL CHECK (ano_fabricacao IS NULL OR ano_fabricacao BETWEEN 1900 AND 2030),
    atualizado_em      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE prf.veiculo IS 'Tabela principal de veículos envolvidos nos acidentes (resolve 2FN separando dados do veículo).';

-- 4.4 Tabela Principal: Envolvido / Pessoa (1 linha por pesid — ~659.445 linhas)
CREATE TABLE prf.envolvido (
    id_pessoa          BIGINT PRIMARY KEY,
    id_acidente        BIGINT NOT NULL REFERENCES prf.acidente(id_acidente) ON DELETE CASCADE,
    id_veiculo         BIGINT NULL REFERENCES prf.veiculo(id_veiculo) ON DELETE SET NULL,
    id_tipo_envolvido  INTEGER NOT NULL REFERENCES prf.tipo_envolvido(id_tipo_envolvido),
    id_estado_fisico   INTEGER NOT NULL REFERENCES prf.estado_fisico(id_estado_fisico),
    idade              SMALLINT NULL CHECK (idade IS NULL OR idade BETWEEN 1 AND 120),
    sexo               VARCHAR(20) NOT NULL,
    atualizado_em      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE prf.envolvido IS 'Tabela principal de pessoas envolvidas (resolve 2FN; id_veiculo é NULL para pedestres/cavaleiros sem veículo).';

-- =============================================================================
-- 5. ÍNDICES DE PERFORMANCE (Consultas analíticas, JOINs e Carga Incremental)
-- =============================================================================
CREATE INDEX idx_acidente_data          ON prf.acidente (data_acidente);
CREATE INDEX idx_acidente_municipio     ON prf.acidente (id_municipio);
CREATE INDEX idx_acidente_uop           ON prf.acidente (id_uop);
CREATE INDEX idx_acidente_causa         ON prf.acidente (id_causa_acidente);
CREATE INDEX idx_acidente_tipo          ON prf.acidente (id_tipo_acidente);
CREATE INDEX idx_acidente_atualizado_em ON prf.acidente (atualizado_em);

CREATE INDEX idx_veiculo_acidente       ON prf.veiculo (id_acidente);
CREATE INDEX idx_veiculo_modelo         ON prf.veiculo (id_modelo_veiculo);
CREATE INDEX idx_veiculo_tipo           ON prf.veiculo (id_tipo_veiculo);

CREATE INDEX idx_envolvido_acidente     ON prf.envolvido (id_acidente);
CREATE INDEX idx_envolvido_veiculo      ON prf.envolvido (id_veiculo);
CREATE INDEX idx_envolvido_estado       ON prf.envolvido (id_estado_fisico);