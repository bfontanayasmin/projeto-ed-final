# projeto-ed-final

Projeto final da disciplina de Estrutura de Dados: um ambiente local de dados, montado com Docker, para processar os registros de acidentes nas rodovias federais brasileiras (2023 a 2026, dados da PRF).

O ambiente junta quatro peças:

- **PostgreSQL:** banco de dados de origem.
- **MinIO:** object storage que faz o papel de data lake.
- **Apache Spark:** cluster com um master e um worker para o processamento.
- **Datasets:** os CSVs de acidentes, já versionados no repositório.

Fluxo dos dados:

```
CSVs (Datasets/) ──COPY──▶ Postgres staging ──ETL 3FN──▶ Postgres schema prf ──Spark (JDBC)──▶ MinIO s3a://bronze/prf/
```

> **Status:** o Postgres sobe já com os CSVs carregados e normalizados (schema `prf`, 18 tabelas). O MinIO cria os buckets `bronze`, `silver` e `gold`, e o job `postgres_para_bronze.py` copia as 18 tabelas para a camada bronze em Parquet. As camadas silver e gold ainda não têm jobs.

## Estrutura do projeto

```
projeto-ed-final/
├── docker-compose.yml          # PostgreSQL, MinIO, Spark master e Spark worker
├── Dockerfile                  # imagem do Spark com hadoop-aws (s3a) e driver JDBC do Postgres
├── conf/spark-defaults.conf    # endpoint do MinIO para o s3a
├── requirements.txt            # dependências Python para desenvolver os jobs fora do container
├── .env.example                # modelo do .env
├── .gitignore
├── README.md
├── Datasets/                   # CSVs de acidentes da PRF (montada no Postgres em /datasets)
│   ├── acidentes2023/acidentes2023.csv
│   ├── acidentes2024/acidentes2024.csv
│   ├── acidentes2025/acidentes2025.csv
│   └── acidentes2026/acidentes2026.csv
├── docs/
│   └── guia_dbeaver_normalizacao.md   # modelo 3FN e execução manual pelo DBeaver
├── scripts/                    # montada no Spark em /opt/spark-apps
│   ├── postgres_para_bronze.py # job: schema prf (Postgres) → s3a://bronze/prf/
│   └── sql/                    # montada no Postgres em /docker-entrypoint-initdb.d
│       ├── 01_ddl_schema_prf.sql
│       ├── 02_carga_staging.sql        # COPY dos 4 CSVs para staging.acidentes_raw
│       ├── 02_etl_normalizacao_prf.sql
│       └── 03_validacao_criterios_prf.sql
└── lake/                       # (ignorada no git) dados locais, montada em /opt/spark-data
```

## Serviços

Todos os containers ficam na rede `ed-network` e se enxergam pelo nome do serviço.

| Serviço | Container | Imagem | Portas no host | Para que serve |
|---|---|---|---|---|
| `postgres` | `ed-postgres` | `postgres:16` | `5090` | Banco de origem (`acidentes_origem`) |
| `minio` | `ed-minio` | `soldevelo/minio:2026.7` | `9000` (API S3), `9001` (console) | Data lake (buckets `bronze`, `silver`, `gold`) |
| `spark-master` | `ed-spark-master` | `ed-spark:3.5.6` (build do `Dockerfile`) | `8080` (UI), `7077` (master) | Coordena o cluster Spark |
| `spark-worker` | `ed-spark-worker` | `ed-spark:3.5.6` | `8081` (UI) | Executa as tarefas (2 cores, 2 GB) |

Volumes:

- **Volumes nomeados:** `postgres_data` e `minio_data`, onde ficam os dados persistidos do banco e do data lake.
- **Pastas do host montadas no Postgres:** `./Datasets` em `/datasets` (somente leitura) e `./scripts/sql` em `/docker-entrypoint-initdb.d`.
- **Pastas do host montadas no master e no worker:** `./scripts` em `/opt/spark-apps` e `./lake` em `/opt/spark-data`.

Como as peças se conectam:

- **Carga do Postgres:** na **primeira** subida, com o volume `postgres_data` vazio, o Postgres roda os scripts de `scripts/sql` em ordem alfabética: `01_ddl` → `02_carga_staging` → `02_etl` → `03_validacao`. A carga leva de 3 a 4 minutos. Nas subidas seguintes os dados já estão no volume e nada é recarregado.
- **Spark → MinIO:** o `s3a://` aponta para `http://minio:9000` (em `conf/spark-defaults.conf`). O usuário e a senha vêm de `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, que o compose preenche com `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`.
- **Spark → Postgres:** os jobs leem `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER` e `POSTGRES_PASSWORD` do ambiente do container.

## Como rodar

### Pré-requisitos

- Docker e Docker Compose v2
- Pelo menos ~4 GB de RAM livres para os containers

### 1. Criar o `.env`

O `.env` está no `.gitignore`, então cada pessoa cria o seu na raiz do projeto, a partir do `.env.example`:

```env
# PostgreSQL
POSTGRES_DB=acidentes_origem
POSTGRES_USER=ed_user
POSTGRES_PASSWORD=troque_esta_senha

# MinIO (a senha precisa ter pelo menos 8 caracteres)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=troque_esta_senha
```

- **`POSTGRES_PASSWORD`:** obrigatória. Sem ela o compose não sobe.
- **`POSTGRES_DB` e `POSTGRES_USER`:** opcionais, os valores padrão são os do exemplo acima.
- **`MINIO_ROOT_USER` e `MINIO_ROOT_PASSWORD`:** não têm valor padrão no compose, então precisam estar no `.env`. Com senha de menos de 8 caracteres o MinIO não sobe e fica repetindo `Adding local Minio host to 'mc' configuration...` no log.

### 2. Subir os serviços

Na primeira vez, o `--build` monta a imagem do Spark (baixa uns 300 MB de jars):

```bash
docker compose up -d --build
```

```bash
docker compose ps
```

O Spark só sobe depois que o Postgres e o MinIO ficam `healthy`, então na primeira vez ele espera a carga do Postgres terminar. Para acompanhar:

```bash
docker logs -f ed-postgres
```

A carga terminou quando aparecer `PostgreSQL init process complete`. Para conferir:

```bash
docker exec -it ed-postgres psql -U ed_user -d acidentes_origem -c "SELECT COUNT(*) FROM prf.acidente;"
```

O esperado é `268392`.

### 3. Acessar

| O quê | Endereço |
|---|---|
| Console do MinIO | http://localhost:9001 (login com `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`) |
| API S3 do MinIO | http://localhost:9000 (`http://minio:9000` de dentro da rede) |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| PostgreSQL | `localhost:5090` (`postgres:5432` de dentro da rede; a porta do host pode ser trocada com `POSTGRES_HOST_PORT` no `.env`) |

### 4. Copiar o Postgres para a camada bronze

```bash
docker exec -it ed-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/postgres_para_bronze.py
```

O job lê as 18 tabelas do schema `prf` e grava cada uma em Parquet em `s3a://bronze/prf/<tabela>/`, com a coluna extra `_ingerido_em`. Ao final de cada tabela ele imprime uma linha `[bronze] prf.<tabela> -> ... (N linhas)`. A execução leva cerca de 1 minuto, e o resultado aparece no console do MinIO, no bucket `bronze`.

Para outros jobs, coloque o script em `scripts/` e troque o nome do arquivo no comando.

### Parar

```bash
docker compose down
```

O comando acima mantém os dados. Para apagar também os volumes do Postgres e do MinIO, use `docker compose down -v`.

Para recarregar só o Postgres do zero (por exemplo, depois de mudar um script em `scripts/sql`), sem apagar o MinIO:

```bash
docker compose rm -sf postgres
```

```bash
docker volume rm projeto-ed-final_postgres_data
```

```bash
docker compose up -d postgres
```

## Datasets

Os arquivos em `Datasets/` são os dados abertos de acidentes da PRF agrupados por pessoa: cada linha é **uma pessoa envolvida**, então o mesmo acidente (`id`) aparece em várias linhas.

| Arquivo | Período | Linhas | Acidentes distintos |
|---|---|---:|---:|
| `acidentes2023.csv` | 01/01/2023 a 31/12/2023 | 183.361 | 67.766 |
| `acidentes2024.csv` | 01/01/2024 a 31/12/2024 | 196.448 | 73.202 |
| `acidentes2025.csv` | 01/01/2025 a 31/12/2025 | 194.629 | 72.529 |
| `acidentes2026.csv` | 01/01/2026 a 30/09/2026 | 148.868 | 54.895 |

O arquivo de 2026 vai só até setembro, então o ano está incompleto.

Formato dos arquivos:

- **Separador:** `;`
- **Codificação:** ISO-8859-1 (Latin-1)
- **Quebra de linha:** CRLF
- **Decimais:** vírgula (ex.: `-19,09484877`)
- **Valores ausentes:** `NA` ou `Não Informado`

O campo `tracado_via` pode ter `;` dentro das aspas (ex.: `"Reta;Declive"`), então a leitura precisa respeitar as aspas.

Os quatro arquivos têm as mesmas 35 colunas:

| Grupo | Colunas |
|---|---|
| Identificação | `id`, `pesid`, `id_veiculo` |
| Data e hora | `data_inversa`, `dia_semana`, `horario`, `fase_dia` |
| Localização | `uf`, `br`, `km`, `municipio`, `latitude`, `longitude`, `sentido_via`, `uso_solo` |
| Acidente | `causa_acidente`, `tipo_acidente`, `classificacao_acidente` |
| Via e clima | `condicao_metereologica`, `tipo_pista`, `tracado_via` |
| Veículo | `tipo_veiculo`, `marca`, `ano_fabricacao_veiculo` |
| Pessoa | `tipo_envolvido`, `estado_fisico`, `idade`, `sexo` |
| Vítimas | `ilesos`, `feridos_leves`, `feridos_graves`, `mortos` |
| Órgão da PRF | `regional`, `delegacia`, `uop` |

Leitura no Spark:

```python
df = (spark.read
      .option("header", True)
      .option("sep", ";")
      .option("encoding", "ISO-8859-1")
      .option("quote", '"')
      .csv("/opt/spark-data/acidentes2023.csv"))
```

O caminho acima supõe que o CSV foi copiado para `lake/`, que é a pasta montada em `/opt/spark-data`. A pasta `Datasets/` não é montada nos containers.

## Próximos passos

- [x] Preencher o `Dockerfile` e o `requirements.txt`
- [x] Criar a pasta `scripts/` com os jobs Spark
- [x] Configurar o Spark para ler e gravar no MinIO (`s3a://`, `hadoop-aws`)
- [x] Carregar os CSVs no PostgreSQL de origem
- [x] Adicionar um `.env.example` ao repositório
- [ ] Job da camada silver (bronze → `s3a://silver/`)
- [ ] Job da camada gold (indicadores em `s3a://gold/`)
