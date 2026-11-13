# projeto-ed-final

Projeto final da disciplina de Estrutura de Dados: um ambiente local de dados, montado com Docker, para processar os registros de acidentes nas rodovias federais brasileiras (2023 a 2026, dados da PRF).

O ambiente junta quatro peças:

- **PostgreSQL:** banco de dados de origem.
- **MinIO:** object storage que faz o papel de data lake.
- **Apache Spark:** cluster com um master e um worker para o processamento.
- **Datasets:** os CSVs de acidentes, já versionados no repositório.

> **Status:** pré-configuração. O `docker-compose.yml` já sobe os serviços, mas o `Dockerfile` e o `requirements.txt` ainda estão vazios, e a pasta `scripts/` (onde vão ficar os jobs Spark) ainda não existe.

## Estrutura do projeto

```
projeto-ed-final/
├── docker-compose.yml   # PostgreSQL, MinIO, Spark master e Spark worker
├── Dockerfile           # (vazio) imagem própria do projeto
├── requirements.txt     # (vazio) dependências Python
├── .gitignore
├── README.md
├── Datasets/            # CSVs de acidentes da PRF
│   ├── acidentes2023/acidentes2023.csv
│   ├── acidentes2024/acidentes2024.csv
│   ├── acidentes2025/acidentes2025.csv
│   └── acidentes2026/acidentes2026.csv
├── scripts/             # (a criar) jobs Spark, montada em /opt/spark-apps
└── lake/                # (ignorada no git) dados locais, montada em /opt/spark-data
```

## Serviços

Todos os containers ficam na rede `ed-network` e se enxergam pelo nome do serviço.

| Serviço | Container | Imagem | Portas no host | Para que serve |
|---|---|---|---|---|
| `postgres` | `ed-postgres` | `postgres:16` | `5432` | Banco de origem (`acidentes_origem`) |
| `minio` | `ed-minio` | `soldevelo/minio:2026.7` | `9000` (API S3), `9001` (console) | Data lake |
| `spark-master` | `ed-spark-master` | `apache/spark:3.5.6` | `8080` (UI), `7077` (master) | Coordena o cluster Spark |
| `spark-worker` | `ed-spark-worker` | `apache/spark:3.5.6` | `8081` (UI) | Executa as tarefas (2 cores, 2 GB) |

Volumes:

- **Volumes nomeados:** `postgres_data` e `minio_data`, onde ficam os dados persistidos do banco e do data lake.
- **Pastas do host montadas no master e no worker:** `./scripts` em `/opt/spark-apps` e `./lake` em `/opt/spark-data`.

## Como rodar

### Pré-requisitos

- Docker e Docker Compose v2
- Pelo menos ~4 GB de RAM livres para os containers

### 1. Criar o `.env`

O `.env` está no `.gitignore`, então cada pessoa cria o seu na raiz do projeto:

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
- **`MINIO_ROOT_USER` e `MINIO_ROOT_PASSWORD`:** não têm valor padrão no compose, então precisam estar no `.env`.

### 2. Subir os serviços

```bash
docker compose up -d
```

```bash
docker compose ps
```

### 3. Acessar

| O quê | Endereço |
|---|---|
| Console do MinIO | http://localhost:9001 (login com `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`) |
| API S3 do MinIO | http://localhost:9000 (`http://minio:9000` de dentro da rede) |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| PostgreSQL | `localhost:5432` (`postgres:5432` de dentro da rede) |

### 4. Enviar um job para o Spark

Com um script em `scripts/`, por exemplo `scripts/meu_job.py`:

```bash
docker exec -it ed-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/meu_job.py
```

### Parar

```bash
docker compose down
```

O comando acima mantém os dados. Para apagar também os volumes do Postgres e do MinIO, use `docker compose down -v`.

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

- [ ] Preencher o `Dockerfile` e o `requirements.txt`
- [ ] Criar a pasta `scripts/` com os jobs Spark
- [ ] Configurar o Spark para ler e gravar no MinIO (`s3a://`, `hadoop-aws`)
- [ ] Carregar os CSVs no PostgreSQL de origem
- [ ] Adicionar um `.env.example` ao repositório
