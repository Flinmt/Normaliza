#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import os
import re
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


def _title_from_rcl_cod(rcl_cod: str) -> str:
    code = (rcl_cod or "").strip()
    upper = code.upper()
    if upper == "TRIAGEM":
        return "Tri@gem Smart"
    if code.startswith("101010"):
        return "Consulta Smart"
    if upper == "FORMDILA":
        return "Dilatação"
    if not code:
        return "Anamnese Migrada"
    return f"{code.title()} Smart"


def _normalize_datetime(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    m = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(\.\d+)?$", raw)
    if not m:
        return raw
    base, frac = m.group(1), m.group(2)
    if not frac:
        return base
    ms = (frac[1:] + "000")[:3]
    return f"{base}.{ms}"


def _sortable_datetime(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "9999-12-31 23:59:59.999999"

    m = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:\.(\d+))?$", raw)
    if not m:
        return "9999-12-31 23:59:59.999999"

    base = m.group(1)
    frac = (m.group(2) or "")
    micros = (frac + "000000")[:6]
    return f"{base}.{micros}"


def _compose_html(rcl_cod: str, rcl_dthr: str, rendered: str) -> str:
    title = _title_from_rcl_cod(rcl_cod)
    dt = html.escape((rcl_dthr or "").strip())
    content = (rendered or "").strip()

    if "<ul>" in content and "<li>" in content:
        content_wo_h2 = re.sub(r"^\s*<h2>.*?</h2>\s*", "", content, count=1, flags=re.S)
        return f"<h3>{html.escape(title)}</h3> <h4>{dt}</h4> {content_wo_h2}".strip()

    # Regra de negócio: texto amplo (sem estrutura @#) deve ser preservado sem alteração.
    return content


def _iter_source_rows_sorted(source: Path):
    fd, sqlite_path = tempfile.mkstemp(prefix="normaliza_sort_", suffix=".sqlite3")
    os.close(fd)

    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE staging (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                sort_key TEXT NOT NULL,
                rcl_pac TEXT,
                rcl_cod TEXT,
                rcl_dthr TEXT,
                rcl_med TEXT,
                rendered TEXT
            )
            """
        )

        insert_sql = """
            INSERT INTO staging (sort_key, rcl_pac, rcl_cod, rcl_dthr, rcl_med, rendered)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        batch: list[tuple[str, str, str, str, str, str]] = []

        with source.open("r", encoding="utf-8", errors="replace", newline="") as rf:
            reader = csv.DictReader(rf)
            for row in reader:
                rcl_dthr = (row.get("rcl_dthr") or "").strip()
                rendered = row.get("rcl_txt_render") or row.get("rcl_txt_html") or row.get("rcl_txt") or ""
                batch.append(
                    (
                        _sortable_datetime(rcl_dthr),
                        (row.get("rcl_pac") or "").strip(),
                        (row.get("rcl_cod") or "").strip(),
                        rcl_dthr,
                        (row.get("rcl_med") or "").strip(),
                        rendered,
                    )
                )
                if len(batch) >= 2000:
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    batch.clear()

        if batch:
            cur.executemany(insert_sql, batch)
            conn.commit()

        for rcl_pac, rcl_cod, rcl_dthr, rcl_med, rendered in cur.execute(
            "SELECT rcl_pac, rcl_cod, rcl_dthr, rcl_med, rendered FROM staging ORDER BY sort_key ASC, seq ASC"
        ):
            yield {
                "rcl_pac": rcl_pac or "",
                "rcl_cod": rcl_cod or "",
                "rcl_dthr": rcl_dthr or "",
                "rcl_med": rcl_med or "",
                "rendered": rendered or "",
            }
    finally:
        conn.close()
        try:
            os.remove(sqlite_path)
        except OSError:
            pass


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gera CSV normalizado para carga na tabela tblAnamnese.")
    p.add_argument("--in", dest="input_csv", default="output/rcl_transformado.csv")
    p.add_argument("--out", dest="output_csv", default="output/tblAnamnese_import.csv")

    p.add_argument("--start-id", type=int, default=None, help="ID inicial manual para intAnamneseId")
    p.add_argument("--auto-start-id", action="store_true", help="Usa MAX(intAnamneseId)+1 do banco")
    p.add_argument("--env", default=".env", help="Arquivo .env (usado com --auto-start-id)")
    p.add_argument("--database", default="BIODATA_HVISAO", help="Banco (usado com --auto-start-id)")

    p.add_argument("--int-usuario-id", type=int, default=1)
    p.add_argument("--int-empresa-id", type=int, default=1)
    p.add_argument("--int-especialidade-medica-id", type=int, default=0)
    p.add_argument("--bol-nao-compartilhar", default="0")
    p.add_argument("--bol-json", default="0")
    p.add_argument("--str-json-default", default="")
    p.add_argument("--bol-triagem-mode", choices=["auto", "null", "zero"], default="auto")
    return p.parse_args()


def resolve_start_id(args: argparse.Namespace) -> int:
    if args.auto_start_id:
        db_conf = load_db_config(args.env)
        conn = _connect(db_conf, args.database)
        try:
            cur = conn.cursor()
            cur.execute("SELECT ISNULL(MAX(intAnamneseId), 0) + 1 FROM dbo.tblAnamnese")
            return int(cur.fetchone()[0])
        finally:
            conn.close()

    if args.start_id is None:
        raise RuntimeError("Informe --start-id ou use --auto-start-id.")
    return int(args.start_id)


def main() -> None:
    args = parse_args()

    source = Path(args.input_csv)
    target = Path(args.output_csv)
    target.parent.mkdir(parents=True, exist_ok=True)

    out_fields = [
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

    current_id = resolve_start_id(args)
    total = 0

    with target.open("w", encoding="utf-8", newline="") as wf:
        writer = csv.DictWriter(wf, fieldnames=out_fields)
        writer.writeheader()

        for row in _iter_source_rows_sorted(source):
            total += 1
            rcl_cod = (row.get("rcl_cod") or "").strip()
            rcl_dthr = (row.get("rcl_dthr") or "").strip()
            rendered = row.get("rendered") or ""

            anamnese_html = _compose_html(rcl_cod, rcl_dthr, rendered)

            if args.bol_triagem_mode == "null":
                bol_triagem = ""
            elif args.bol_triagem_mode == "zero":
                bol_triagem = "0"
            else:
                bol_triagem = "1" if rcl_cod.upper() == "TRIAGEM" else "0"

            out_row = {
                "intAnamneseId": str(current_id),
                "strAnamnese": anamnese_html,
                "intClienteId": (row.get("rcl_pac") or "").strip(),
                "intAtendimentoId": "",
                "intProfissionalId": (row.get("rcl_med") or "").strip(),
                "intUsuarioId": str(args.int_usuario_id),
                "intEmpresaId": str(args.int_empresa_id),
                "datAnamnese": _normalize_datetime(rcl_dthr),
                "strAnamneseMobile": anamnese_html,
                "intEspecialidadeMedicaId": str(args.int_especialidade_medica_id),
                "bolNaoCompartilhar": str(args.bol_nao_compartilhar),
                "bolJson": str(args.bol_json),
                "strJson": args.str_json_default,
                "bolTriagem": bol_triagem,
            }
            writer.writerow(out_row)
            current_id += 1

    print("Concluído.")
    print(f"Registros gerados: {total}")
    print(f"Próximo intAnamneseId disponível: {current_id}")
    print(f"CSV: {target}")


if __name__ == "__main__":
    main()
