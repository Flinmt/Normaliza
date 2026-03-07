#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from normaliza.config import load_db_config
from normaliza.db import _connect


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Cria backup da tblAnamnese com nome tblAnamnese_BKP_ddMMyyyy."
    )
    p.add_argument("--env", default=".env", help="Arquivo .env com credenciais")
    p.add_argument("--database", default="BIODATA_HVISAO", help="Banco de destino")
    p.add_argument(
        "--date",
        default="",
        help="Data no formato ddMMyyyy (padrao: hoje)",
    )
    return p.parse_args()


def resolve_date_token(date_str: str) -> str:
    if not date_str:
        return datetime.now().strftime("%d%m%Y")
    if not re.fullmatch(r"\d{8}", date_str):
        raise RuntimeError("--date deve estar no formato ddMMyyyy, ex.: 03032026")
    return date_str


def main() -> None:
    args = parse_args()
    date_token = resolve_date_token(args.date)
    table_name = f"tblAnamnese_BKP_{date_token}"

    db_conf = load_db_config(args.env)
    conn = _connect(db_conf, args.database)
    conn.autocommit = False

    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM sys.tables
            WHERE name = ?
            """,
            (table_name,),
        )
        exists = int(cur.fetchone()[0]) > 0
        if exists:
            raise RuntimeError(f"Tabela já existe: dbo.{table_name}")

        sql = f"SELECT * INTO dbo.[{table_name}] FROM dbo.tblAnamnese"
        cur.execute(sql)
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM dbo.[{table_name}]")
        total = int(cur.fetchone()[0])

        print("Backup criado com sucesso.")
        print(f"Tabela: dbo.{table_name}")
        print(f"Registros copiados: {total}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
