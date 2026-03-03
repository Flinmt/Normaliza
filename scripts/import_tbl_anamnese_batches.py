#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from normaliza.config import load_db_config
from normaliza.db import _connect


COLS = [
    "intAnamneseId",
    "strAnamnese",
    "intClienteId",
    "intAtendimentoId",
    "intProfissionalId",
    "intUsuarioId",
    "intEmpresaId",
    "datAnamnese",
    "strAnamneseMobile",
    "intEspecialidadeMedicaId",
    "bolNaoCompartilhar",
    "bolJson",
    "strJson",
    "bolTriagem",
]


def as_nullable_int(value: str):
    v = (value or "").strip()
    if not v:
        return None
    return int(v)


def as_nullable_str(value: str):
    v = value if value is not None else ""
    return v if v != "" else None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Importa CSV da tblAnamnese em lotes.")
    p.add_argument("--csv", default="output/tblAnamnese_import.csv", help="CSV de entrada")
    p.add_argument("--env", default=".env", help="Arquivo .env com credenciais")
    p.add_argument("--database", default="BIODATA_HVISAO", help="Banco de destino")
    p.add_argument("--batch-size", type=int, default=200, help="Tamanho do lote")
    p.add_argument(
        "--fast-executemany",
        action="store_true",
        help="Ativa fast_executemany (mais rapido, pode usar mais memoria)",
    )
    p.add_argument("--dry-run", action="store_true", help="Somente valida leitura sem inserir")
    return p.parse_args()


def row_to_tuple(row: dict[str, str]):
    return (
        as_nullable_int(row.get("intAnamneseId")),
        as_nullable_str(row.get("strAnamnese")),
        as_nullable_int(row.get("intClienteId")),
        as_nullable_int(row.get("intAtendimentoId")),
        as_nullable_int(row.get("intProfissionalId")),
        as_nullable_int(row.get("intUsuarioId")),
        as_nullable_int(row.get("intEmpresaId")),
        as_nullable_str(row.get("datAnamnese")),
        as_nullable_str(row.get("strAnamneseMobile")),
        as_nullable_int(row.get("intEspecialidadeMedicaId")),
        as_nullable_str(row.get("bolNaoCompartilhar")),
        as_nullable_str(row.get("bolJson")),
        as_nullable_str(row.get("strJson")),
        as_nullable_str(row.get("bolTriagem")),
    )


def insert_batch_with_fallback(cur, sql: str, batch: list[tuple], min_chunk: int = 10) -> None:
    if not batch:
        return
    try:
        cur.executemany(sql, batch)
        return
    except MemoryError:
        if len(batch) <= min_chunk:
            raise
        mid = len(batch) // 2
        insert_batch_with_fallback(cur, sql, batch[:mid], min_chunk=min_chunk)
        insert_batch_with_fallback(cur, sql, batch[mid:], min_chunk=min_chunk)


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise RuntimeError(f"CSV não encontrado: {csv_path}")

    with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        missing = [c for c in COLS if c not in (reader.fieldnames or [])]
        if missing:
            raise RuntimeError(f"CSV sem colunas obrigatórias: {', '.join(missing)}")

        if args.dry_run:
            count = sum(1 for _ in reader)
            print(f"Dry-run OK. Registros lidos: {count}")
            return

    db_conf = load_db_config(args.env)
    conn = _connect(db_conf, args.database)
    conn.autocommit = False

    sql = """
        INSERT INTO dbo.tblAnamnese (
            intAnamneseId,
            strAnamnese,
            intClienteId,
            intAtendimentoId,
            intProfissionalId,
            intUsuarioId,
            intEmpresaId,
            datAnamnese,
            strAnamneseMobile,
            intEspecialidadeMedicaId,
            bolNaoCompartilhar,
            bolJson,
            strJson,
            bolTriagem
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    inserted = 0
    batch: list[tuple] = []

    try:
        cur = conn.cursor()
        cur.fast_executemany = bool(args.fast_executemany)

        with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                batch.append(row_to_tuple(row))
                if len(batch) >= args.batch_size:
                    insert_batch_with_fallback(cur, sql, batch)
                    conn.commit()
                    inserted += len(batch)
                    print(f"Lote inserido. Total acumulado: {inserted}")
                    batch.clear()

            if batch:
                insert_batch_with_fallback(cur, sql, batch)
                conn.commit()
                inserted += len(batch)
                print(f"Lote final inserido. Total acumulado: {inserted}")

        print("Importação concluída.")
        print(f"Total inserido: {inserted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
