from __future__ import annotations

import argparse
import json
from datetime import datetime

from normaliza.config import load_db_config
from normaliza.db import load_atr_lookup, load_psv_professional_lookup
from normaliza.transform import transform_csv


DEFAULT_DATABASE = "BIODATA_HVISAO"


def main() -> None:
    parser = argparse.ArgumentParser(description="Transforma rcl.csv para versão legível.")
    parser.add_argument("--csv", default="rcl.csv", help="Caminho do CSV de origem")
    parser.add_argument("--env", default=".env", help="Caminho do arquivo .env")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help="Banco SQL Server")
    parser.add_argument("--out", default="output/rcl_transformado.csv", help="CSV de saída")
    parser.add_argument(
        "--summary",
        default="output/transform_summary.json",
        help="Resumo JSON de execução",
    )
    args = parser.parse_args()

    started = datetime.now()
    print("[1/3] Carregando configuração, ATR e PSV...")
    db_config = load_db_config(args.env)
    atr_lookup = load_atr_lookup(db_config, args.database)
    psv_lookup = load_psv_professional_lookup(db_config, args.database)

    print("[2/3] Transformando CSV em streaming...")
    stats = transform_csv(args.csv, args.out, atr_lookup, psv_lookup)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database": args.database,
        "csv": args.csv,
        "output": args.out,
        "atr_pairs_loaded": len(atr_lookup),
        "psv_professional_keys_loaded": len(psv_lookup),
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
