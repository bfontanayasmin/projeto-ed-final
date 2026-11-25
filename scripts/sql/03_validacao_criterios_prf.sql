-- =============================================================================
-- PROJETO: Normalização de Dados Abertos da PRF (2023–2026)
-- SGBD: PostgreSQL (execução via DBeaver)
-- ETAPA: 03 - Validação dos Critérios do Trabalho Final
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. CRITÉRIO: Mínimo de 10 tabelas no banco relacional de origem
--    Resultado esperado: 18 tabelas no schema "prf"
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_tabelas_normalizadas,
    CASE WHEN COUNT(*) >= 10 THEN 'ATENDE (>= 10)' ELSE 'NÃO ATENDE' END AS status_criterio
FROM information_schema.tables
WHERE table_schema = 'prf'
  AND table_type = 'BASE TABLE';

-- -----------------------------------------------------------------------------
-- 2. CRITÉRIO: Mínimo de 10.000 linhas por tabela principal
--    Lista exata de registros em todas as 18 tabelas do schema "prf"
-- -----------------------------------------------------------------------------
SELECT '01. prf.acidente (Principal)'             AS tabela, COUNT(*) AS total_linhas FROM prf.acidente
UNION ALL SELECT '02. prf.acidente_tracado_via (1FN N:N)',   COUNT(*) FROM prf.acidente_tracado_via
UNION ALL SELECT '03. prf.veiculo (Principal)',              COUNT(*) FROM prf.veiculo
UNION ALL SELECT '04. prf.envolvido (Principal)',            COUNT(*) FROM prf.envolvido
UNION ALL SELECT '05. prf.modelo_veiculo (Domínio 3FN)',     COUNT(*) FROM prf.modelo_veiculo
UNION ALL SELECT '06. prf.municipio (Domínio 3FN)',          COUNT(*) FROM prf.municipio
UNION ALL SELECT '07. prf.uop_prf (Domínio 3FN)',            COUNT(*) FROM prf.uop_prf
UNION ALL SELECT '08. prf.fabricante_veiculo (Domínio 3FN)', COUNT(*) FROM prf.fabricante_veiculo
UNION ALL SELECT '09. prf.delegacia_prf (Domínio 3FN)',      COUNT(*) FROM prf.delegacia_prf
UNION ALL SELECT '10. prf.causa_acidente (Domínio)',         COUNT(*) FROM prf.causa_acidente
UNION ALL SELECT '11. prf.regional_prf (Domínio 3FN)',       COUNT(*) FROM prf.regional_prf
UNION ALL SELECT '12. prf.uf (Domínio 3FN)',                 COUNT(*) FROM prf.uf
UNION ALL SELECT '13. prf.tipo_veiculo (Domínio)',           COUNT(*) FROM prf.tipo_veiculo
UNION ALL SELECT '14. prf.tipo_acidente (Domínio)',          COUNT(*) FROM prf.tipo_acidente
UNION ALL SELECT '15. prf.tracado_via (Domínio 1FN)',        COUNT(*) FROM prf.tracado_via
UNION ALL SELECT '16. prf.condicao_meteorologica (Domínio)', COUNT(*) FROM prf.condicao_meteorologica
UNION ALL SELECT '17. prf.tipo_envolvido (Domínio)',         COUNT(*) FROM prf.tipo_envolvido
UNION ALL SELECT '18. prf.estado_fisico (Domínio)',          COUNT(*) FROM prf.estado_fisico
ORDER BY total_linhas DESC;

-- -----------------------------------------------------------------------------
-- 3. CRITÉRIO: Distribuição de datas nos últimos 3 anos
--    Resultado esperado: 2023, 2024, 2025 e 2026 (2023-01-01 a 2026-09-30)
-- -----------------------------------------------------------------------------
SELECT
    EXTRACT(YEAR FROM a.data_acidente)::INTEGER AS ano,
    MIN(a.data_acidente)                        AS primeira_data,
    MAX(a.data_acidente)                        AS ultima_data,
    COUNT(DISTINCT a.id_acidente)               AS total_acidentes,
    COUNT(DISTINCT v.id_veiculo)                AS total_veiculos,
    COUNT(DISTINCT e.id_pessoa)                 AS total_envolvidos
FROM prf.acidente a
LEFT JOIN prf.veiculo v   ON v.id_acidente = a.id_acidente
LEFT JOIN prf.envolvido e ON e.id_acidente = a.id_acidente
GROUP BY 1
ORDER BY 1;

-- -----------------------------------------------------------------------------
-- 4. RECONCILIAÇÃO STAGING vs. SCHEMA NORMALIZADO (Garantia de zero perda)
-- -----------------------------------------------------------------------------
SELECT
    (SELECT COUNT(*) FROM staging.acidentes_raw)                                       AS linhas_staging,
    (SELECT COUNT(DISTINCT CAST(TRIM(id) AS NUMERIC)::BIGINT) FROM staging.acidentes_raw) AS ids_distintos_staging,
    (SELECT COUNT(*) FROM prf.acidente)                                                AS linhas_prf_acidente,
    (SELECT COUNT(*) FROM prf.veiculo)                                                 AS linhas_prf_veiculo,
    (SELECT COUNT(*) FROM prf.envolvido)                                               AS linhas_prf_envolvido,
    (SELECT COUNT(*) FROM prf.acidente_tracado_via)                                    AS linhas_prf_tracado;

-- -----------------------------------------------------------------------------
-- 5. CONSULTA ANALÍTICA DE EXEMPLO (Prévia dos KPIs a partir do modelo 3FN)
--    Demonstra que ilesos/feridos/mortos são derivados dinamicamente de estado_fisico
-- -----------------------------------------------------------------------------
SELECT
    EXTRACT(YEAR FROM a.data_acidente)::INTEGER AS ano,
    COUNT(DISTINCT a.id_acidente)               AS qtd_acidentes,
    COUNT(*) FILTER (WHERE ef.descricao = 'Ileso')         AS ilesos,
    COUNT(*) FILTER (WHERE ef.descricao = 'Lesões Leves')  AS feridos_leves,
    COUNT(*) FILTER (WHERE ef.descricao = 'Lesões Graves') AS feridos_graves,
    COUNT(*) FILTER (WHERE ef.descricao = 'Óbito')         AS mortos,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE ef.descricao = 'Óbito')
        / NULLIF(COUNT(DISTINCT a.id_acidente), 0),
        2
    ) AS obitos_por_100_acidentes
FROM prf.acidente a
JOIN prf.envolvido e       ON e.id_acidente = a.id_acidente
JOIN prf.estado_fisico ef  ON ef.id_estado_fisico = e.id_estado_fisico
GROUP BY 1
ORDER BY 1;