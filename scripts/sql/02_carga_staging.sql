-- =============================================================================
-- PROJETO: Normalização de Dados Abertos da PRF (2023–2026)
-- ETAPA: 02a - Carga dos 4 CSVs da PRF em staging.acidentes_raw
-- =============================================================================
-- Roda automaticamente no Docker, entre o 01_ddl e o 02_etl (o nome "02_carga" ordena antes de "02_etl"), quando o volume
-- do Postgres está vazio. A pasta ./Datasets do projeto fica montada em /datasets.
-- Resultado esperado: 723.306 linhas em staging.acidentes_raw.
-- =============================================================================

TRUNCATE TABLE staging.acidentes_raw;

COPY staging.acidentes_raw FROM '/datasets/acidentes2023/acidentes2023.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"', ENCODING 'LATIN1');
COPY staging.acidentes_raw FROM '/datasets/acidentes2024/acidentes2024.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"', ENCODING 'LATIN1');
COPY staging.acidentes_raw FROM '/datasets/acidentes2025/acidentes2025.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"', ENCODING 'LATIN1');
COPY staging.acidentes_raw FROM '/datasets/acidentes2026/acidentes2026.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"', ENCODING 'LATIN1');
