# Banco de origem

O banco de origem utiliza **PostgreSQL**. Os registros dos CSVs são carregados em uma tabela de staging e organizados em um modelo normalizado no schema `prf`.

O DBeaver é a ferramenta utilizada para acessar o PostgreSQL e executar os scripts SQL.

## Organização dos dados

A estrutura integrada ao repositório contém **18 tabelas**:

| Grupo | Tabelas |
|---|---|
| Entidades e associação | `acidente`, `veiculo`, `envolvido`, `acidente_tracado_via` |
| Localização | `uf`, `municipio` |
| Organização da PRF | `regional_prf`, `delegacia_prf`, `uop_prf` |
| Características do acidente | `causa_acidente`, `tipo_acidente`, `condicao_meteorologica`, `tracado_via` |
| Veículos e pessoas | `tipo_veiculo`, `fabricante_veiculo`, `modelo_veiculo`, `tipo_envolvido`, `estado_fisico` |

A separação permite representar acidentes, veículos e pessoas sem repetir todos os atributos em cada registro. As chaves relacionam as entidades e as tabelas de domínio.

O traçado da via possui uma tabela de associação para representar os vários traçados vinculados a um acidente.

## Scripts disponíveis

Os arquivos estão em [scripts/sql](https://github.com/bfontanayasmin/projeto-ed-final/tree/main/scripts/sql):

| Arquivo | Finalidade |
|---|---|
| `01_ddl_schema_prf.sql` | Criar os schemas e a estrutura das tabelas |
| `02_etl_normalizacao_prf.sql` | Transformar e carregar os dados da staging no modelo normalizado |
| `03_validacao_criterios_prf.sql` | Consultar contagens e conferir os critérios do banco de origem |

## Modelo e procedimento

O [guia de normalização e execução no DBeaver](guia_dbeaver_normalizacao.md), já produzido pelo grupo, reúne:

- Justificativa da normalização.
- Diagrama entidade-relacionamento.
- Procedimento de importação dos CSVs.
- Ordem de execução dos scripts.
- Contagens e consultas de validação registradas na implementação.

O guia foi incorporado à navegação sem alterar seu conteúdo. As contagens nele registradas devem ser acompanhadas pelas evidências de execução na entrega.

