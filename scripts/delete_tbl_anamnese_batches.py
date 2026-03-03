#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from normaliza.config import load_db_config
from normaliza.db import _connect


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Remove registros da tblAnamnese em lotes."
    )
    p.add_argument("--env", default=".env", help="Arquivo .env com credenciais")
    p.add_argument("--database", default="BIODATA_HVISAO", help="Banco de destino")
    p.add_argument("--batch-size", type=int, default=2000, help="Tamanho do lote")
    p.add_argument(
        "--yes",
        action="store_true",
        help="Confirma a exclusao sem prompt interativo",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Somente mostra quantidade atual sem excluir",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.batch_size <= 0:
        raise RuntimeError("--batch-size deve ser maior que zero")

    db_conf = load_db_config(args.env)
    conn = _connect(db_conf, args.database)
    conn.autocommit = False

    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dbo.tblAnamnese")
        total_before = int(cur.fetchone()[0])
        print(f"Registros atuais em dbo.tblAnamnese: {total_before}")

        if args.dry_run:
            print("Dry-run concluido. Nenhum registro foi excluido.")
            return

        if not args.yes:
            answer = input(
                "Confirmar exclusao em lotes de TODOS os registros de dbo.tblAnamnese? (digite SIM): "
            ).strip()
            if answer != "SIM":
                print("Operacao cancelada.")
                return

        deleted_total = 0
        while True:
            # batch_size e validado como inteiro positivo.
            sql = f"DELETE TOP ({args.batch_size}) FROM dbo.tblAnamnese"
            cur.execute(sql)
            deleted = int(cur.rowcount if cur.rowcount is not None else 0)
            conn.commit()

            if deleted <= 0:
                break

            deleted_total += deleted
            print(f"Lote removido: {deleted}. Total removido: {deleted_total}")

        cur.execute("SELECT COUNT(*) FROM dbo.tblAnamnese")
        total_after = int(cur.fetchone()[0])
        print("Exclusao concluida.")
        print(f"Total removido: {deleted_total}")
        print(f"Registros restantes: {total_after}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
