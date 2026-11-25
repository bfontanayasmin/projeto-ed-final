# Dataset

## Origem

O projeto utiliza os dados públicos da Polícia Rodoviária Federal (PRF) sobre acidentes nas rodovias federais brasileiras.

Os arquivos de 2023, 2024, 2025 e 2026 estão disponíveis na pasta [Datasets do repositório](https://github.com/bfontanayasmin/projeto-ed-final/tree/main/Datasets). A identificação exata da versão e do endereço de download será acrescentada ao registro da fonte.

## Período e volume

O levantamento registrado no README apresenta os seguintes valores:

| Arquivo | Período | Linhas | Acidentes distintos |
|---|---|---:|---:|
| acidentes2023.csv | 01/01/2023 a 31/12/2023 | 183.361 | 67.766 |
| acidentes2024.csv | 01/01/2024 a 31/12/2024 | 196.448 | 73.202 |
| acidentes2025.csv | 01/01/2025 a 31/12/2025 | 194.629 | 72.529 |
| acidentes2026.csv | 01/01/2026 a 30/09/2026 | 148.868 | 54.895 |

Os quatro arquivos somam **723.306 linhas**. Essas contagens reproduzem o levantamento já registrado pelo grupo; não representam uma nova execução de validação feita para esta página.

!!! note "Ano de 2026"
    O arquivo de 2026 cobre somente até setembro. As comparações temporais precisam considerar essa diferença de cobertura.

## O que representa uma linha

Os arquivos são agrupados por pessoa envolvida. Por isso, um mesmo acidente pode aparecer em várias linhas, com pessoas e veículos associados a ele.

Assim, contar linhas do CSV é diferente de contar acidentes. Para analisar acidentes, é necessário considerar seus identificadores distintos e evitar duplicações ao combinar tabelas.

## Características dos arquivos

| Característica | Configuração registrada no projeto |
|---|---|
| Formato | CSV com cabeçalho |
| Separador | Ponto e vírgula (`;`) |
| Codificação | ISO-8859-1 (Latin-1) |
| Casas decimais | Vírgula |
| Campos entre aspas | Podem conter o próprio separador |
| Valores ausentes | Incluem `NA` e `Não Informado` |

O campo `tracado_via`, por exemplo, pode conter mais de um valor separado por ponto e vírgula dentro de aspas. A leitura precisa respeitar essas aspas.

## Informações disponíveis

Os dados incluem identificadores do acidente, veículo e pessoa, além de:

- Data, horário e localização.
- Causa, tipo e classificação do acidente.
- Condições da via e do clima.
- Características do veículo.
- Características e estado físico das pessoas envolvidas.
- Identificação da unidade da PRF.

As regras de separação desses dados no banco são descritas no [guia de normalização](guia_dbeaver_normalizacao.md).

