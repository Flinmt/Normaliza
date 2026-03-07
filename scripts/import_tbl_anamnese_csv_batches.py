#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
import tempfile
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
    p.add_argument(
        "--no-resume",
        action="store_true",
        help="Desativa retomada automatica por intAnamneseId ja existente",
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


def sortable_datetime_desc(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "0000-00-00 00:00:00.000000"
    if "." in raw:
        base, frac = raw.split(".", 1)
    else:
        base, frac = raw, ""
    frac = (frac + "000000")[:6]
    return f"{base}.{frac}"


def iter_rows_sorted_recent_first(csv_path: Path):
    fd, sqlite_path = tempfile.mkstemp(prefix="normaliza_import_", suffix=".sqlite3")
    os.close(fd)
    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE staging (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                sort_key TEXT NOT NULL,
                intAnamneseId TEXT,
                strAnamnese TEXT,
                intClienteId TEXT,
                intAtendimentoId TEXT,
                intProfissionalId TEXT,
                intUsuarioId TEXT,
                intEmpresaId TEXT,
                datAnamnese TEXT,
                strAnamneseMobile TEXT,
                intEspecialidadeMedicaId TEXT,
                bolNaoCompartilhar TEXT,
                bolJson TEXT,
                strJson TEXT,
                bolTriagem TEXT
            )
            """
        )

        insert_sql = """
            INSERT INTO staging (
                sort_key, intAnamneseId, strAnamnese, intClienteId, intAtendimentoId,
                intProfissionalId, intUsuarioId, intEmpresaId, datAnamnese, strAnamneseMobile,
                intEspecialidadeMedicaId, bolNaoCompartilhar, bolJson, strJson, bolTriagem
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        batch: list[tuple[str, ...]] = []
        with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                batch.append(
                    (
                        sortable_datetime_desc(row.get("datAnamnese") or ""),
                        *(row.get(col) or "" for col in COLS),
                    )
                )
                if len(batch) >= 1000:
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    batch.clear()
            if batch:
                cur.executemany(insert_sql, batch)
                conn.commit()

        select_sql = """
            SELECT
                intAnamneseId, strAnamnese, intClienteId, intAtendimentoId, intProfissionalId,
                intUsuarioId, intEmpresaId, datAnamnese, strAnamneseMobile,
                intEspecialidadeMedicaId, bolNaoCompartilhar, bolJson, strJson, bolTriagem
            FROM staging
            ORDER BY sort_key DESC, seq DESC
        """
        for row in cur.execute(select_sql):
            yield {COLS[idx]: row[idx] or "" for idx in range(len(COLS))}
    finally:
        conn.close()
        try:
            os.remove(sqlite_path)
        except OSError:
            pass


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


def fetch_existing_ids(cur, ids: list[int]) -> set[int]:
    if not ids:
        return set()
    placeholders = ",".join("?" for _ in ids)
    sql = f"SELECT intAnamneseId FROM dbo.tblAnamnese WHERE intAnamneseId IN ({placeholders})"
    cur.execute(sql, ids)
    return {int(row[0]) for row in cur.fetchall()}


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
    skipped_resume = 0
    skipped_db_duplicate = 0
    skipped_csv_duplicate = 0
    batch: list[tuple] = []
    seen_in_csv: set[int] = set()

    try:
        cur = conn.cursor()
        cur.fast_executemany = bool(args.fast_executemany)
        resume_enabled = not args.no_resume
        if resume_enabled:
            print("Resume ativo. Registros ja existentes no banco serao ignorados por intAnamneseId.")

        for row in iter_rows_sorted_recent_first(csv_path):
            item = row_to_tuple(row)
            row_id = item[0]
            if row_id is None:
                continue

            if row_id in seen_in_csv:
                skipped_csv_duplicate += 1
                continue
            seen_in_csv.add(row_id)
            batch.append(item)

            if len(batch) >= args.batch_size:
                batch_ids = [x[0] for x in batch if x[0] is not None]
                existing_ids = fetch_existing_ids(cur, batch_ids)
                if existing_ids:
                    if resume_enabled:
                        skipped_resume += sum(1 for x in batch if x[0] in existing_ids)
                    else:
                        skipped_db_duplicate += sum(1 for x in batch if x[0] in existing_ids)
                to_insert = [x for x in batch if x[0] not in existing_ids]
                if to_insert:
                    insert_batch_with_fallback(cur, sql, to_insert)
                conn.commit()
                inserted += len(to_insert)
                print(f"Lote inserido. Total acumulado: {inserted}")
                batch.clear()

        if batch:
            batch_ids = [x[0] for x in batch if x[0] is not None]
            existing_ids = fetch_existing_ids(cur, batch_ids)
            if existing_ids:
                if resume_enabled:
                    skipped_resume += sum(1 for x in batch if x[0] in existing_ids)
                else:
                    skipped_db_duplicate += sum(1 for x in batch if x[0] in existing_ids)
            to_insert = [x for x in batch if x[0] not in existing_ids]
            if to_insert:
                insert_batch_with_fallback(cur, sql, to_insert)
            conn.commit()
            inserted += len(to_insert)
            print(f"Lote final inserido. Total acumulado: {inserted}")

        print("Importação concluída.")
        print(f"Total inserido: {inserted}")
        print(f"Ignorados por resume: {skipped_resume}")
        print(f"Ignorados por duplicidade no banco: {skipped_db_duplicate}")
        print(f"Ignorados por duplicidade no CSV: {skipped_csv_duplicate}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
