from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from normaliza.config import load_db_config
from normaliza.db import (
    load_atr_lookup,
    load_client_id_lookup,
    load_psv_professional_lookup,
)
from normaliza.transform import transform_csv


DEFAULT_DATABASE = "BIODATA_HVISAO"
DEFAULT_CLIENT_MAP_DATABASE = "REPOSITORIO_HVISAO"


def resolve_input_encoding(csv_path: str, requested: str) -> str:
    if requested.lower() != "auto":
        return requested

    sample = Path(csv_path).read_bytes()[:512_000]
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def main() -> None:
    parser = argparse.ArgumentParser(description="Transforma rcl.csv para versão legível.")
    parser.add_argument("--csv", default="data/initial/initial.csv", help="Caminho do CSV de origem")
    parser.add_argument("--env", default=".env", help="Caminho do arquivo .env")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help="Banco SQL Server")
    parser.add_argument(
        "--client-map-database",
        default=DEFAULT_CLIENT_MAP_DATABASE,
        help="Banco onde está tblMap_PACReg_ClienteId",
    )
    parser.add_argument("--out", default="output/rcl_transformado.csv", help="CSV de saída")
    parser.add_argument(
        "--summary",
        default="output/transform_summary.json",
        help="Resumo JSON de execução",
    )
    parser.add_argument(
        "--input-encoding",
        default="auto",
        help="Encoding do CSV de entrada (ex.: auto, utf-8, cp1252, latin-1)",
    )
    parser.add_argument(
        "--output-encoding",
        default="utf-8",
        help="Encoding do CSV de saída",
    )
    args = parser.parse_args()

    started = datetime.now()
    print("[1/3] Carregando configuração, ATR, PSV e mapa de cliente...")
    db_config = load_db_config(args.env)
    atr_lookup = load_atr_lookup(db_config, args.database)
    psv_lookup = load_psv_professional_lookup(db_config, args.database)
    client_lookup = load_client_id_lookup(db_config, args.database, args.client_map_database)
    input_encoding = resolve_input_encoding(args.csv, args.input_encoding)
    print(f"Encoding de entrada resolvido: {input_encoding}")

    print("[2/3] Transformando CSV em streaming...")
    stats = transform_csv(
        args.csv,
        args.out,
        atr_lookup,
        psv_lookup,
        client_lookup,
        input_encoding=input_encoding,
        output_encoding=args.output_encoding,
    )

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database": args.database,
        "client_map_database": args.client_map_database,
        "csv": args.csv,
        "output": args.out,
        "input_encoding": input_encoding,
        "output_encoding": args.output_encoding,
        "atr_pairs_loaded": len(atr_lookup),
        "psv_professional_keys_loaded": len(psv_lookup),
        "client_id_map_keys_loaded": len(client_lookup),
        "stats": stats,
        "elapsed_seconds": (datetime.now() - started).total_seconds(),
    }

    with open(args.summary, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("[3/3] Concluído")
    print(f"Saída: {args.out}")
    print(f"Resumo: {args.summary}")


if __name__ == "__main__":
    main()
