# Databricks - pré-configuração

Bundle do Databricks (Databricks Asset Bundles) do projeto. Pensado para o
**Databricks Free Edition** (compute serverless, catálogo `workspace`), mas funciona
em qualquer workspace com Unity Catalog — basta trocar a variável `catalog`.

```
databricks/
├── databricks.yml                       # configuração do bundle (variáveis e targets)
└── notebooks/
    └── 00_setup.py                      # cria schemas bronze/silver/gold e o volume raw
```

## Estrutura no Unity Catalog

| Objeto | Para que serve |
|---|---|
| `workspace.bronze.raw` (volume) | CSVs originais em `/Volumes/workspace/bronze/raw/acidentes/` |
| `workspace.bronze` | (a fazer) tabelas com o dado bruto dos CSVs |
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
```

O deploy envia os notebooks para `/Workspace/Users/<seu_email>/.bundle/projeto_ed_final/dev/files/notebooks/`.
Abra o `00_setup` no workspace e clique em **Run all** para criar os schemas e o volume.

## 4. Enviar os CSVs para o volume

Da raiz do repositório:

```powershell
databricks fs cp -r Datasets dbfs:/Volumes/workspace/bronze/raw/acidentes --overwrite
```

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
