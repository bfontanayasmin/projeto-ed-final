# Databricks - pré-configuração

Bundle do Databricks (Databricks Asset Bundles) do projeto. Pensado para o
**Databricks Free Edition** (compute serverless, catálogo `workspace`), mas funciona
em outros workspaces com Unity Catalog. Para usar outro catálogo ou outros schemas,
configure as variáveis do bundle, os widgets do notebook e o caminho de upload
conforme a seção **Trocar catálogo ou schemas**.

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
databricks auth login --host https://<seu-workspace>.cloud.databricks.com --profile projeto-ed-final
```

Use o perfil `projeto-ed-final` em todos os comandos abaixo para acessar o mesmo workspace.

Isso abre o navegador e grava o perfil em `~/.databrickscfg` (fica fora do repositório — não commite tokens).

## 3. Publicar e rodar o setup

Todos os comandos `bundle` são rodados de dentro da pasta `databricks/`:

```powershell
cd databricks
databricks bundle validate --profile projeto-ed-final
databricks bundle deploy --profile projeto-ed-final
```

O deploy envia os notebooks para `/Workspace/Users/<seu_email>/.bundle/projeto_ed_final/dev/files/notebooks/`.
Com os valores padrão (`workspace`, `bronze`, `silver`, `gold` e `raw`), abra o
`00_setup` no workspace e clique em **Run all** para criar os schemas e o volume.
Se tiver alterado catálogo, schemas ou volume, configure primeiro os widgets como
explicado abaixo. O catálogo escolhido precisa existir, e seu usuário precisa ter
as permissões necessárias para criar schemas e volumes.

## 4. Enviar os CSVs para o volume

Da raiz do repositório:

```powershell
databricks fs cp -r Datasets dbfs:/Volumes/workspace/bronze/raw/acidentes --overwrite --profile projeto-ed-final
```

## Trocar catálogo ou schemas

Você pode alterar as variáveis do bundle no deploy, sem editar o arquivo:

```powershell
databricks bundle deploy --var="catalog=meu_catalogo" --profile projeto-ed-final
```

**As variáveis do bundle não preenchem automaticamente os widgets quando o
notebook é executado manualmente.** Antes de criar os objetos:

1. Abra o notebook `00_setup` publicado pelo deploy.
2. Execute somente a primeira célula Python, que cria os widgets.
3. No widget `catalog`, informe `meu_catalogo`. Se também alterou schemas ou
   volume, preencha `schema_bronze`, `schema_silver`, `schema_gold` e `volume_raw`
   com os mesmos valores escolhidos para o ambiente.
4. Execute as células seguintes, de criação dos schemas e do volume, e depois a
   célula que prepara e exibe o diretório de upload.
5. Confira os nomes dos schemas e o caminho impressos pelo notebook antes de
   enviar os arquivos.

Por exemplo, usando `meu_catalogo` e mantendo `bronze` e `raw`, o comando de
upload, executado da raiz do repositório, fica:

```powershell
databricks fs cp -r Datasets dbfs:/Volumes/meu_catalogo/bronze/raw/acidentes --overwrite --profile projeto-ed-final
```

Se mudar o schema Bronze ou o volume, ajuste também esses trechos do caminho.
Use o diretório exibido pelo notebook, acrescentando o prefixo `dbfs:` ao caminho
`/Volumes/...` para o comando da CLI.

## Observações sobre os CSVs da PRF

- Separador `;`, encoding **ISO-8859-1** (latin1). Ler como UTF-8 quebra os acentos.
- `km`, `latitude` e `longitude` usam vírgula como decimal — converter na silver
  (`regexp_replace(col, ',', '.').cast('double')`).
- Uma linha por **pessoa envolvida**; o mesmo acidente (`id`) se repete.
