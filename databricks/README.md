# Databricks - pré-configuração

Bundle do Databricks (Databricks Asset Bundles) do projeto. Pensado para o
**Databricks Free Edition** (compute serverless, catálogo `workspace`), mas funciona
em qualquer workspace com Unity Catalog — basta trocar a variável `catalog`.

```
databricks/
├── databricks.yml                       # configuração do bundle (variáveis e targets)
├── resources/
│   └── acidentes_pipeline.job.yml       # job: setup -> bronze
└── notebooks/
    ├── 00_setup.py                      # cria schemas bronze/silver/gold e o volume raw
    └── 01_bronze_acidentes.py           # lê os CSVs e grava a tabela bronze.acidentes
```

## Estrutura no Unity Catalog

| Objeto | Para que serve |
|---|---|
| `workspace.bronze.raw` (volume) | CSVs originais em `/Volumes/workspace/bronze/raw/acidentes/` |
| `workspace.bronze.acidentes` | Dado bruto, tudo string + `_arquivo_origem` e `_data_ingestao` |
| `workspace.silver` | (a fazer) dado limpo e tipado |
| `workspace.gold` | (a fazer) agregações para análise |

## 1. Instalar a CLI do Databricks (uma vez)

Windows:

```powershell
winget install Databricks.DatabricksCLI
```

Linux / WSL (script oficial da Databricks; não existe pacote no `apt`):

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
```

macOS:

```bash
brew tap databricks/tap && brew install databricks
```

Confira com `databricks --version` (precisa ser 0.218 ou mais nova).

## 2. Logar no workspace

Crie a conta em https://www.databricks.com/learn/free-edition, copie a URL do workspace e rode:

```powershell
databricks auth login --host https://<seu-workspace>.cloud.databricks.com
```

Isso abre o navegador e grava o perfil em `~/.databrickscfg` (fica fora do repositório — não commite tokens).

## 3. Publicar e rodar o setup

Todos os comandos `bundle` são rodados de dentro da pasta `databricks/`:

```powershell
cd databricks
databricks bundle validate
databricks bundle deploy
databricks bundle run acidentes_pipeline --only setup
```

## 4. Enviar os CSVs para o volume

Da raiz do repositório:

```powershell
databricks fs cp -r Datasets dbfs:/Volumes/workspace/bronze/raw/acidentes --overwrite
```

## 5. Rodar a ingestão bronze

```powershell
cd databricks
databricks bundle run acidentes_pipeline
```

No workspace, o job aparece em **Jobs & Pipelines** como `[dev <seu_usuario>] acidentes_pipeline`,
e os notebooks ficam em `/Workspace/Users/<seu_email>/.bundle/projeto_ed_final/dev/`.

## Trocar catálogo ou schemas

Sem editar arquivo:

```powershell
databricks bundle deploy --var="catalog=meu_catalogo"
```

## Observações sobre os CSVs da PRF

- Separador `;`, encoding **ISO-8859-1** (latin1). Ler como UTF-8 quebra os acentos.
- `km`, `latitude` e `longitude` usam vírgula como decimal — converter na silver
  (`regexp_replace(col, ',', '.').cast('double')`).
- Uma linha por **pessoa envolvida**; o mesmo acidente (`id`) se repete.
