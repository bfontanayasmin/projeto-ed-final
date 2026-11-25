-- =============================================================================
-- PROJETO: Normalização de Dados Abertos da PRF (2023–2026)
-- SGBD: PostgreSQL (execução via DBeaver)
-- ETAPA: 02 - Carga e Normalização (staging.acidentes_raw -> schema prf)
-- =============================================================================
-- Pré-requisito:
--   Importar acidentes2023.csv, acidentes2024.csv, acidentes2025.csv e
--   acidentes2026.csv para a tabela staging.acidentes_raw (723.306 linhas).
--
-- Tratamentos aplicados automaticamente neste script:
--   1. Notação científica em IDs (ex.: "6e+05", "8e+05") -> CAST(... AS NUMERIC)::BIGINT
--   2. Valores sentinela ('NA', 'N/A', '(null)', '') -> NULL
--   3. Vírgula decimal brasileira em km, latitude, longitude -> ponto decimal
--   4. Atomicização 1FN da coluna multivalorada tracado_via (separada por ";")
--   5. Separação de marca ("FABRICANTE/MODELO") em fabricante_veiculo + modelo_veiculo
--   6. Remoção de veículos/pessoas sentinela (id_veiculo = '0' / pesid = '0')
--   7. Higienização de ano_fabricacao = 0 e idade fora de [1..120] -> NULL
--   8. Idempotência via ON CONFLICT DO NOTHING (permite reexecução segura)
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. CARGA DAS TABELAS DE DOMÍNIO E HIERARQUIA (3FN)
-- -----------------------------------------------------------------------------

-- 1.1 UF
INSERT INTO prf.uf (sigla_uf)
SELECT DISTINCT UPPER(TRIM(uf))
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(uf), '') IS NOT NULL
  AND UPPER(TRIM(uf)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (sigla_uf) DO NOTHING;

-- 1.2 Município
INSERT INTO prf.municipio (sigla_uf, nome_municipio)
SELECT DISTINCT
    UPPER(TRIM(uf)),
    TRIM(municipio)
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(uf), '') IS NOT NULL
  AND NULLIF(TRIM(municipio), '') IS NOT NULL
ON CONFLICT (sigla_uf, nome_municipio) DO NOTHING;

-- 1.3 Regional PRF
INSERT INTO prf.regional_prf (codigo_regional)
SELECT DISTINCT TRIM(regional)
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(regional), '') IS NOT NULL
  AND UPPER(TRIM(regional)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (codigo_regional) DO NOTHING;

-- 1.4 Delegacia PRF
INSERT INTO prf.delegacia_prf (codigo_delegacia, id_regional)
SELECT DISTINCT
    TRIM(s.delegacia),
    r.id_regional
FROM staging.acidentes_raw s
JOIN prf.regional_prf r
  ON r.codigo_regional = TRIM(s.regional)
WHERE NULLIF(TRIM(s.delegacia), '') IS NOT NULL
  AND UPPER(TRIM(s.delegacia)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (codigo_delegacia) DO NOTHING;

-- 1.5 UOP PRF
INSERT INTO prf.uop_prf (codigo_uop, id_delegacia)
SELECT DISTINCT
    TRIM(s.uop),
    d.id_delegacia
FROM staging.acidentes_raw s
JOIN prf.delegacia_prf d
  ON d.codigo_delegacia = TRIM(s.delegacia)
WHERE NULLIF(TRIM(s.uop), '') IS NOT NULL
  AND UPPER(TRIM(s.uop)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (codigo_uop) DO NOTHING;

-- 1.6 Causa do Acidente
INSERT INTO prf.causa_acidente (descricao)
SELECT DISTINCT TRIM(causa_acidente)
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(causa_acidente), '') IS NOT NULL
  AND UPPER(TRIM(causa_acidente)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (descricao) DO NOTHING;

-- 1.7 Tipo de Acidente
INSERT INTO prf.tipo_acidente (descricao)
SELECT DISTINCT TRIM(tipo_acidente)
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(tipo_acidente), '') IS NOT NULL
  AND UPPER(TRIM(tipo_acidente)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (descricao) DO NOTHING;

-- 1.8 Condição Meteorológica
INSERT INTO prf.condicao_meteorologica (descricao)
SELECT DISTINCT TRIM(condicao_metereologica)
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(condicao_metereologica), '') IS NOT NULL
  AND UPPER(TRIM(condicao_metereologica)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (descricao) DO NOTHING;

-- 1.9 Traçado da Via (valores atômicos — 1FN)
INSERT INTO prf.tracado_via (descricao)
SELECT DISTINCT TRIM(item)
FROM staging.acidentes_raw s,
     LATERAL REGEXP_SPLIT_TO_TABLE(s.tracado_via, ';') AS item
WHERE NULLIF(TRIM(item), '') IS NOT NULL
  AND UPPER(TRIM(item)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (descricao) DO NOTHING;

-- 1.10 Tipo de Veículo
INSERT INTO prf.tipo_veiculo (descricao)
SELECT DISTINCT TRIM(tipo_veiculo)
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(tipo_veiculo), '') IS NOT NULL
  AND UPPER(TRIM(tipo_veiculo)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (descricao) DO NOTHING;

-- 1.11 Fabricante de Veículo (parte anterior à primeira "/" em marca)
INSERT INTO prf.fabricante_veiculo (nome_fabricante)
SELECT DISTINCT
    CASE
        WHEN POSITION('/' IN TRIM(marca)) > 0
            THEN TRIM(SPLIT_PART(TRIM(marca), '/', 1))
        ELSE TRIM(marca)
    END AS nome_fabricante
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(marca), '') IS NOT NULL
  AND UPPER(TRIM(marca)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (nome_fabricante) DO NOTHING;

-- 1.12 Modelo de Veículo (parte posterior à primeira "/" em marca)
INSERT INTO prf.modelo_veiculo (id_fabricante, nome_modelo)
SELECT DISTINCT
    f.id_fabricante,
    CASE
        WHEN POSITION('/' IN TRIM(s.marca)) > 0
            THEN TRIM(SUBSTRING(TRIM(s.marca) FROM POSITION('/' IN TRIM(s.marca)) + 1))
        ELSE 'Não Informado'
    END AS nome_modelo
FROM staging.acidentes_raw s
JOIN prf.fabricante_veiculo f
  ON f.nome_fabricante = CASE
        WHEN POSITION('/' IN TRIM(s.marca)) > 0
            THEN TRIM(SPLIT_PART(TRIM(s.marca), '/', 1))
        ELSE TRIM(s.marca)
     END
WHERE NULLIF(TRIM(s.marca), '') IS NOT NULL
  AND UPPER(TRIM(s.marca)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (id_fabricante, nome_modelo) DO NOTHING;

-- 1.13 Tipo de Envolvido
INSERT INTO prf.tipo_envolvido (descricao)
SELECT DISTINCT TRIM(tipo_envolvido)
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(tipo_envolvido), '') IS NOT NULL
  AND UPPER(TRIM(tipo_envolvido)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (descricao) DO NOTHING;

-- 1.14 Estado Físico
INSERT INTO prf.estado_fisico (descricao)
SELECT DISTINCT TRIM(estado_fisico)
FROM staging.acidentes_raw
WHERE NULLIF(TRIM(estado_fisico), '') IS NOT NULL
  AND UPPER(TRIM(estado_fisico)) NOT IN ('NA', 'N/A', '(NULL)')
ON CONFLICT (descricao) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 2. CARGA DAS TABELAS PRINCIPAIS E ASSOCIATIVA (1FN e 2FN)
-- -----------------------------------------------------------------------------

-- 2.1 Tabela Principal: Acidente (1 linha por id_acidente)
INSERT INTO prf.acidente (
    id_acidente,
    data_acidente,
    horario,
    id_municipio,
    br,
    km,
    latitude,
    longitude,
    id_uop,
    id_causa_acidente,
    id_tipo_acidente,
    id_condicao_meteorologica,
    classificacao_acidente,
    fase_dia,
    sentido_via,
    tipo_pista,
    uso_solo_urbano
)
SELECT DISTINCT ON (CAST(TRIM(s.id) AS NUMERIC)::BIGINT)
    CAST(TRIM(s.id) AS NUMERIC)::BIGINT                                    AS id_acidente,
    CAST(TRIM(s.data_inversa) AS DATE)                                     AS data_acidente,
    CAST(TRIM(s.horario) AS TIME)                                          AS horario,
    m.id_municipio,
    NULLIF(CAST(TRIM(s.br) AS NUMERIC)::SMALLINT, 0)                       AS br,
    CAST(REPLACE(TRIM(s.km), ',', '.') AS NUMERIC(6, 1))                   AS km,
    CAST(REPLACE(TRIM(s.latitude), ',', '.') AS NUMERIC(11, 8))            AS latitude,
    CAST(REPLACE(TRIM(s.longitude), ',', '.') AS NUMERIC(11, 8))           AS longitude,
    u.id_uop,
    c.id_causa_acidente,
    t.id_tipo_acidente,
    cm.id_condicao_meteorologica,
    CASE
        WHEN UPPER(TRIM(s.classificacao_acidente)) IN ('NA', 'N/A', '(NULL)', '')
            THEN NULL
        ELSE TRIM(s.classificacao_acidente)
    END                                                                    AS classificacao_acidente,
    TRIM(s.fase_dia)                                                       AS fase_dia,
    TRIM(s.sentido_via)                                                    AS sentido_via,
    TRIM(s.tipo_pista)                                                     AS tipo_pista,
    (LOWER(TRIM(s.uso_solo)) = 'sim')                                      AS uso_solo_urbano
FROM staging.acidentes_raw s
JOIN prf.municipio m
  ON m.sigla_uf = UPPER(TRIM(s.uf))
 AND m.nome_municipio = TRIM(s.municipio)
JOIN prf.causa_acidente c
  ON c.descricao = TRIM(s.causa_acidente)
JOIN prf.tipo_acidente t
  ON t.descricao = TRIM(s.tipo_acidente)
JOIN prf.condicao_meteorologica cm
  ON cm.descricao = TRIM(s.condicao_metereologica)
LEFT JOIN prf.uop_prf u
  ON u.codigo_uop = TRIM(s.uop)
 AND UPPER(TRIM(s.uop)) NOT IN ('NA', 'N/A', '(NULL)', '')
ORDER BY CAST(TRIM(s.id) AS NUMERIC)::BIGINT
ON CONFLICT (id_acidente) DO NOTHING;

-- 2.2 Tabela Associativa N:N (1FN): Acidente <-> Traçado da Via
INSERT INTO prf.acidente_tracado_via (id_acidente, id_tracado_via)
SELECT DISTINCT
    CAST(TRIM(s.id) AS NUMERIC)::BIGINT AS id_acidente,
    tv.id_tracado_via
FROM staging.acidentes_raw s,
     LATERAL REGEXP_SPLIT_TO_TABLE(s.tracado_via, ';') AS item
JOIN prf.tracado_via tv
  ON tv.descricao = TRIM(item)
ON CONFLICT (id_acidente, id_tracado_via) DO NOTHING;

-- 2.3 Tabela Principal: Veículo (ignora id_veiculo = '0', que indica pedestre sem veículo)
INSERT INTO prf.veiculo (
    id_veiculo,
    id_acidente,
    id_tipo_veiculo,
    id_modelo_veiculo,
    ano_fabricacao
)
SELECT DISTINCT ON (CAST(TRIM(s.id_veiculo) AS NUMERIC)::BIGINT)
    CAST(TRIM(s.id_veiculo) AS NUMERIC)::BIGINT AS id_veiculo,
    CAST(TRIM(s.id) AS NUMERIC)::BIGINT         AS id_acidente,
    tv.id_tipo_veiculo,
    mv.id_modelo_veiculo,
    CASE
        WHEN TRIM(s.ano_fabricacao_veiculo) ~ '^[0-9]+$'
         AND CAST(TRIM(s.ano_fabricacao_veiculo) AS INTEGER) BETWEEN 1900 AND 2030
            THEN CAST(TRIM(s.ano_fabricacao_veiculo) AS SMALLINT)
        ELSE NULL
    END AS ano_fabricacao
FROM staging.acidentes_raw s
JOIN prf.fabricante_veiculo fv
  ON fv.nome_fabricante = CASE
        WHEN POSITION('/' IN TRIM(s.marca)) > 0
            THEN TRIM(SPLIT_PART(TRIM(s.marca), '/', 1))
        ELSE TRIM(s.marca)
     END
JOIN prf.modelo_veiculo mv
  ON mv.id_fabricante = fv.id_fabricante
 AND mv.nome_modelo = CASE
        WHEN POSITION('/' IN TRIM(s.marca)) > 0
            THEN TRIM(SUBSTRING(TRIM(s.marca) FROM POSITION('/' IN TRIM(s.marca)) + 1))
        ELSE 'Não Informado'
     END
LEFT JOIN prf.tipo_veiculo tv
  ON tv.descricao = TRIM(s.tipo_veiculo)
 AND UPPER(TRIM(s.tipo_veiculo)) NOT IN ('NA', 'N/A', '(NULL)', '')
WHERE TRIM(s.id_veiculo) ~ '^[0-9]+(\.[0-9]+)?([eE]\+?[0-9]+)?$'
  AND CAST(TRIM(s.id_veiculo) AS NUMERIC)::BIGINT > 0
ORDER BY CAST(TRIM(s.id_veiculo) AS NUMERIC)::BIGINT
ON CONFLICT (id_veiculo) DO NOTHING;

-- 2.4 Tabela Principal: Envolvido / Pessoa (ignora pesid = '0', que indica veículo sem pessoa)
INSERT INTO prf.envolvido (
    id_pessoa,
    id_acidente,
    id_veiculo,
    id_tipo_envolvido,
    id_estado_fisico,
    idade,
    sexo
)
SELECT DISTINCT ON (CAST(TRIM(s.pesid) AS NUMERIC)::BIGINT)
    CAST(TRIM(s.pesid) AS NUMERIC)::BIGINT AS id_pessoa,
    CAST(TRIM(s.id) AS NUMERIC)::BIGINT    AS id_acidente,
    CASE
        WHEN TRIM(s.id_veiculo) ~ '^[0-9]+(\.[0-9]+)?([eE]\+?[0-9]+)?$'
         AND CAST(TRIM(s.id_veiculo) AS NUMERIC)::BIGINT > 0
            THEN CAST(TRIM(s.id_veiculo) AS NUMERIC)::BIGINT
        ELSE NULL
    END AS id_veiculo,
    te.id_tipo_envolvido,
    ef.id_estado_fisico,
    CASE
        WHEN TRIM(s.idade) ~ '^[0-9]+$'
         AND CAST(TRIM(s.idade) AS INTEGER) BETWEEN 1 AND 120
            THEN CAST(TRIM(s.idade) AS SMALLINT)
        ELSE NULL
    END AS idade,
    TRIM(s.sexo) AS sexo
FROM staging.acidentes_raw s
JOIN prf.tipo_envolvido te
  ON te.descricao = TRIM(s.tipo_envolvido)
JOIN prf.estado_fisico ef
  ON ef.descricao = TRIM(s.estado_fisico)
WHERE TRIM(s.pesid) ~ '^[0-9]+(\.[0-9]+)?([eE]\+?[0-9]+)?$'
  AND CAST(TRIM(s.pesid) AS NUMERIC)::BIGINT > 0
ORDER BY CAST(TRIM(s.pesid) AS NUMERIC)::BIGINT
ON CONFLICT (id_pessoa) DO NOTHING;

COMMIT;

-- Atualiza estatísticas do otimizador do PostgreSQL após a carga em massa
ANALYZE prf.uf;
ANALYZE prf.municipio;
ANALYZE prf.regional_prf;
ANALYZE prf.delegacia_prf;
ANALYZE prf.uop_prf;
ANALYZE prf.causa_acidente;
ANALYZE prf.tipo_acidente;
ANALYZE prf.condicao_meteorologica;
ANALYZE prf.tracado_via;
ANALYZE prf.tipo_veiculo;
ANALYZE prf.fabricante_veiculo;
ANALYZE prf.modelo_veiculo;
ANALYZE prf.tipo_envolvido;
ANALYZE prf.estado_fisico;
ANALYZE prf.acidente;
ANALYZE prf.acidente_tracado_via;
ANALYZE prf.veiculo;
ANALYZE prf.envolvido;