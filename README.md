# Normaliza

Pipeline de normalização/migração do histórico clínico (`rcl.csv`) do sistema legado para consumo no banco `BIODATA_HVISAO`.

## Objetivo

- Decodificar tokens estruturados `@#<codigo>@<campo><marcador><valor>` usando a tabela `ATR`.
- Gerar saída legível em HTML puro (sem CSS).
- Preservar o texto original (`rcl_txt`) sem alterações.

## Estrutura

- `src/normaliza/config.py`: leitura de configuração (`.env`).
- `src/normaliza/db.py`: conexão SQL Server e carga de dicionário ATR.
- `src/normaliza/decoder.py`: parser e renderização HTML.
- `src/normaliza/transform.py`: processamento streaming do CSV.
- `scripts/transform_rcl.py`: CLI principal.

## Requisitos

1. Python 3.11+
2. Dependências:

```bash
pip install -r requirements.txt
```

3. Driver ODBC SQL Server instalado no Windows (`ODBC Driver 18` ou `17`).

## Configuração (`.env`)

Exemplo:

```env
DB_HOST=...
DB_PORT=1433
DB_USER=...
DB_PASS=...
```

## Execução

```bash
python scripts/transform_rcl.py --csv rcl.csv --env .env --database BIODATA_HVISAO --out output/rcl_transformado.csv
```

## Saída

O CSV de saída mantém as colunas originais e adiciona:

- `rcl_med_original`: valor original do identificador legado do médico (`PSV_COD`).
- `rcl_txt_html`: versão legível (somente decodificação estruturada).
- `rcl_txt_original`: cópia fiel do `rcl_txt` original.
- `rcl_txt_render`: campo final sugerido para consumo.
  - Se houver estrutura `@#...`: usa `rcl_txt_html`.
  - Caso contrário: usa `rcl_txt_original` (sem alterar texto amplo).

## Regras implementadas

- `TRIAGEM` é renomeado para `TRI@GEM` no título do bloco HTML.
- Títulos iniciados por `101010` são convertidos para `CONSULTA`.
- Todos os títulos recebem o sufixo ` - Migrado`.
- `rcl_med` é substituído por `intProfissionalId` via tabela `PSV` (`PSV_COD` -> `intProfissionalId`).
- Campos sem mapeamento em `ATR` são mantidos como fallback: `Campo <número>`.
- Tokens não estruturados (ex.: `@#24` em texto livre) não são tratados como estrutura.

## Observações

- A tabela `ATR` é a fonte principal de mapeamento de legibilidade.
- O texto livre não é reformulado para evitar perda de contexto clínico.

