#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

# Padrões encontrados no rcl_txt:
# @#20887@18ON
# @#20887@1&SIM
# @#92@1%-1500,0
TOKEN_CODE_RE = re.compile(r"@#(\d+)")
TOKEN_FULL_RE = re.compile(r"@#(\d+)@(\d+)([A-Z%&])")

DEFAULT_DB = "BIODATA_HVISAO"


def set_max_csv_field_size():
    # On Windows, sys.maxsize can exceed C long accepted by csv.field_size_limit.
    # Decrease until we find the highest accepted value.
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10
            if limit <= 0:
                csv.field_size_limit(1024 * 1024)
                return


def load_env(path):
    env = {}
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def connect_sqlserver(env, database):
    try:
        import pyodbc
    except ImportError:
        raise RuntimeError(
            "pyodbc não está instalado. Instale com: pip install pyodbc"
        )

    host = env.get("DB_HOST")
    port = env.get("DB_PORT", "1433")
    user = env.get("DB_USER")
    pwd = env.get("DB_PASS")

    if not all([host, user, pwd]):
        raise RuntimeError("DB_HOST, DB_USER e DB_PASS são obrigatórios no .env")

    installed = set(pyodbc.drivers())
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    candidates = [d for d in preferred if d in installed]

    if not candidates:
        raise RuntimeError(
            "Nenhum driver ODBC SQL Server encontrado. "
            f"Drivers instalados: {', '.join(sorted(installed)) or '(nenhum)'}"
        )

    last_error = None
    for driver in candidates:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};PWD={pwd};"
            "Encrypt=no;TrustServerCertificate=yes;"
        )
        try:
            return pyodbc.connect(conn_str, timeout=30)
        except Exception as exc:
            last_error = exc

    raise RuntimeError(
        "Não foi possível conectar com os drivers detectados "
        f"({', '.join(candidates)}). Erro final: {last_error}"
    )


def fetch_set(conn, sql, col=0):
    out = set()
    cur = conn.cursor()
    cur.execute(sql)
    for row in cur:
        v = row[col]
        if v is not None:
            out.add(str(v).strip())
    return out


def fetch_dsc_lookup(conn, used_codes):
    if not used_codes:
        return {}

    cur = conn.cursor()
    code_list = sorted(used_codes)
    lookup = {}

    # SQL Server limita quantidade de parâmetros por query; fazemos em blocos.
    chunk_size = 800
    for i in range(0, len(code_list), chunk_size):
        chunk = code_list[i : i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        sql = f"""
            SELECT DSC_COD, DSC_TIPO, DSC_ROT
            FROM dsc
            WHERE DSC_COD IN ({placeholders})
        """
        cur.execute(sql, chunk)
        for dsc_cod, dsc_tipo, dsc_rot in cur.fetchall():
            lookup[str(dsc_cod).strip()] = {
                "DSC_TIPO": "" if dsc_tipo is None else str(dsc_tipo),
                "DSC_ROT": "" if dsc_rot is None else str(dsc_rot),
            }

    return lookup


def parse_csv(csv_path):
    stats = {
        "rows_total": 0,
        "rows_with_rcl_txt": 0,
        "rows_encoded": 0,
        "rows_plain_only": 0,
        "rows_empty_txt": 0,
    }

    code_counter = Counter()
    code_field_counter = Counter()  # (code, field_id, marker)
    rcl_cod_counter = Counter()  # valor da coluna rcl_cod
    code_by_rcl_cod = defaultdict(set)

    # Aumenta limite para colunas/textos grandes (compatível com Windows)
    set_max_csv_field_size()

    with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        required = {"rcl_cod", "rcl_txt"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(
                f"CSV sem colunas esperadas. Faltando: {', '.join(sorted(missing))}"
            )

        for row in reader:
            stats["rows_total"] += 1

            rcl_cod = (row.get("rcl_cod") or "").strip()
            rcl_txt = row.get("rcl_txt") or ""

            if rcl_cod:
                rcl_cod_counter[rcl_cod] += 1

            if not rcl_txt.strip():
                stats["rows_empty_txt"] += 1
                continue

            stats["rows_with_rcl_txt"] += 1

            has_encoded = False

            # Códigos @#<número>
            for m in TOKEN_CODE_RE.finditer(rcl_txt):
                code = m.group(1)
                code_counter[code] += 1
                has_encoded = True
                if rcl_cod:
                    code_by_rcl_cod[rcl_cod].add(code)

            # Estrutura completa @#<code>@<field><marker>
            for m in TOKEN_FULL_RE.finditer(rcl_txt):
                code_field_counter[(m.group(1), m.group(2), m.group(3))] += 1

            if has_encoded:
                stats["rows_encoded"] += 1
            else:
                stats["rows_plain_only"] += 1

    return stats, code_counter, code_field_counter, rcl_cod_counter, code_by_rcl_cod


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def write_csv(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def main():
    parser = argparse.ArgumentParser(
        description="Gera relatórios de mapeamento do rcl.csv com dsc/dic."
    )
    parser.add_argument("--csv", default="rcl.csv", help="Caminho do rcl.csv")
    parser.add_argument("--env", default=".env", help="Caminho do .env")
    parser.add_argument("--database", default=DEFAULT_DB, help="Banco SQL Server")
    parser.add_argument("--out", default="reports_mapping", help="Pasta de saída")
    args = parser.parse_args()

    t0 = datetime.now()

    ensure_dir(args.out)

    env = load_env(args.env)
    print("[1/4] Lendo CSV em streaming...")
    (
        stats,
        code_counter,
        code_field_counter,
        rcl_cod_counter,
        code_by_rcl_cod,
    ) = parse_csv(args.csv)

    used_codes = set(code_counter.keys())

    print("[2/4] Conectando no SQL Server e carregando dicionários...")
    conn = connect_sqlserver(env, args.database)
    try:
        dsc_codes = fetch_set(conn, "SELECT DISTINCT DSC_COD FROM dsc")
        dic_dsc_codes = fetch_set(conn, "SELECT DISTINCT DIC_DSC FROM dic")
        dsc_lookup = fetch_dsc_lookup(conn, used_codes)
    finally:
        conn.close()

    print("[3/4] Cruzando cobertura de mapeamento...")
    missing_in_dsc = sorted(used_codes - dsc_codes)
    missing_in_dic = sorted(used_codes - dic_dsc_codes)
    present_in_dsc = sorted(used_codes & dsc_codes)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database": args.database,
        "csv_file": os.path.abspath(args.csv),
        "stats": stats,
        "unique_codes_in_csv": len(used_codes),
        "unique_codes_in_dsc_table": len(dsc_codes),
        "unique_codes_in_dic_table(DIC_DSC)": len(dic_dsc_codes),
        "codes_present_in_dsc": len(present_in_dsc),
        "codes_missing_in_dsc": len(missing_in_dsc),
        "codes_missing_in_dic": len(missing_in_dic),
        "top_20_codes_in_csv": code_counter.most_common(20),
        "top_20_rcl_cod": rcl_cod_counter.most_common(20),
        "elapsed_seconds": (datetime.now() - t0).total_seconds(),
    }

    print("[4/4] Gravando relatórios...")

    # summary.json
    with open(os.path.join(args.out, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # code_coverage.csv
    rows = []
    for code, count in code_counter.most_common():
        dsc_info = dsc_lookup.get(code, {})
        rows.append(
            [
                code,
                count,
                "Y" if code in dsc_codes else "N",
                "Y" if code in dic_dsc_codes else "N",
                dsc_info.get("DSC_TIPO", ""),
                dsc_info.get("DSC_ROT", ""),
            ]
        )

    write_csv(
        os.path.join(args.out, "code_coverage.csv"),
        ["code", "occurrences_in_csv", "exists_in_dsc", "exists_in_dic_dsc", "DSC_TIPO", "DSC_ROT"],
        rows,
    )

    # missing reports
    write_csv(
        os.path.join(args.out, "missing_in_dsc.csv"),
        ["code"],
        [[c] for c in missing_in_dsc],
    )
    write_csv(
        os.path.join(args.out, "missing_in_dic.csv"),
        ["code"],
        [[c] for c in missing_in_dic],
    )

    # code_field_patterns.csv: granularidade @#code@field marker
    pattern_rows = [
        [code, field_id, marker, count]
        for (code, field_id, marker), count in code_field_counter.most_common()
    ]
    write_csv(
        os.path.join(args.out, "code_field_patterns.csv"),
        ["code", "field_id", "marker", "occurrences"],
        pattern_rows,
    )

    # rcl_cod_to_codes.csv
    map_rows = []
    for rcod, cnt in rcl_cod_counter.most_common():
        codes = sorted(code_by_rcl_cod.get(rcod, set()))
        map_rows.append([rcod, cnt, len(codes), ",".join(codes)])
    write_csv(
        os.path.join(args.out, "rcl_cod_to_codes.csv"),
        ["rcl_cod", "rows", "unique_codes_found", "codes_list"],
        map_rows,
    )

    print("Concluído.")
    print(f"Relatórios em: {os.path.abspath(args.out)}")
    print(f"Tempo total: {(datetime.now() - t0).total_seconds():.1f}s")


if __name__ == "__main__":
    main()
