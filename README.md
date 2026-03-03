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
- `scripts/build_tbl_anamnese_csv.py`: exporta CSV final no layout de `tblAnamnese`.
- `scripts/import_tbl_anamnese_batches.py`: importa o CSV final na `tblAnamnese` em lotes.
- `scripts/delete_tbl_anamnese_batches.py`: remove dados da `tblAnamnese` em lotes.

## Requisitos

1. Python 3.11+
2. Dependências:

```bash
pip install -r requirements.txt
```

3. Driver ODBC SQL Server instalado no Windows (`ODBC Driver 18` ou `17`).

## Docker

Build da imagem:

```bash
docker build -t normaliza:latest .
```

Executar menu interativo:

```bash
docker run --rm -it -v "$(pwd):/app" normaliza:latest
```

No PowerShell (Windows), use:

```powershell
docker run --rm -it -v "${PWD}:/app" normaliza:latest
```

Com Docker Compose:

```bash
docker compose up --build
```

Observações:
- `rcl.csv` e `.env` devem existir no diretório do projeto host para serem usados no container.
- o volume `./:/app` permite ler e gerar arquivos localmente em `output/`.

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
python scripts/transform_rcl.py --csv rcl.csv --env .env --database BIODATA_HVISAO --client-map-database REPOSITORIO_HVISAO --out output/rcl_transformado.csv
```

## Menu Interativo (Terminal)

Para executar tudo a partir de um único comando com opções numeradas:

```bash
python scripts/menu.py
```

Opções disponíveis no menu:
- Transformar CSV legado.
- Gerar preview do CSV original.
- Gerar preview do CSV transformado.
- Transformar e gerar preview em sequência.
- Gerar CSV para `tblAnamnese` com `intAnamneseId` manual.
- Importar CSV para `tblAnamnese` em lotes.
- Remover dados da `tblAnamnese` em lotes.

## Saída

O CSV de saída mantém as colunas originais e adiciona:

- `rcl_pac_original`: valor original legado do paciente.
- `rcl_med_original`: valor original do identificador legado do médico (`PSV_COD`).
- `rcl_txt_html`: versão legível (somente decodificação estruturada).
- `rcl_txt_original`: cópia fiel do `rcl_txt` original.
- `rcl_txt_render`: campo final sugerido para consumo.
  - Se houver estrutura `@#...`: usa `rcl_txt_html`.
  - Caso contrário: usa `rcl_txt_original` (sem alterar texto amplo).

## Exportação para tblAnamnese

Para gerar um CSV já no formato da tabela `tblAnamnese`:

```bash
python scripts/build_tbl_anamnese_csv.py --in output/rcl_transformado.csv --out output/tblAnamnese_import.csv --start-id 400000
```

Observação:
- `intAnamneseId` é manual e sequencial a partir de `--start-id`.

Para importar em lotes:

```bash
python scripts/import_tbl_anamnese_batches.py --csv output/tblAnamnese_import.csv --env .env --database BIODATA_HVISAO --batch-size 2000
```

Para remover todos os dados de `tblAnamnese` em lotes:

```bash
python scripts/delete_tbl_anamnese_batches.py --env .env --database BIODATA_HVISAO --batch-size 2000
```

Aviso:
- comando destrutivo; use com cuidado.
- o script pede confirmacao interativa (`SIM`) por padrao.

## Regras implementadas

- `TRIAGEM` é renomeado para `TRI@GEM` no título do bloco HTML.
- Títulos iniciados por `101010` são convertidos para `CONSULTA`.
- Todos os títulos recebem o sufixo ` - Migrado`.
- `rcl_med` é substituído por `intProfissionalId` via tabela `PSV` (`PSV_COD` -> `intProfissionalId`).
- `rcl_pac` é substituído por `clientid_new` via tabela `REPOSITORIO_HVISAO.dbo.tblMap_PACReg_ClienteId` (`pac_reg_old` -> `clientid_new`).
- Campos sem mapeamento em `ATR` são mantidos como fallback: `Campo <número>`.
- Tokens não estruturados (ex.: `@#24` em texto livre) não são tratados como estrutura.

## Observações

- A tabela `ATR` é a fonte principal de mapeamento de legibilidade.
- O texto livre não é reformulado para evitar perda de contexto clínico.

