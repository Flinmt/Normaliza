#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


def detect_encoding(path: Path, requested: str) -> str:
    if requested.lower() != "auto":
        return requested
    sample = path.read_bytes()[:512_000]
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def extract_parenthesized(text: str, open_paren_idx: int) -> tuple[str, int]:
    depth = 0
    in_str = False
    i = open_paren_idx
    start = open_paren_idx + 1

    while i < len(text):
        ch = text[i]
        if in_str:
            if ch == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    i += 2
                    continue
                in_str = False
            i += 1
            continue

        if ch == "'":
            in_str = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[start:i], i
        i += 1

    raise RuntimeError("Bloco VALUES com parenteses nao fechado.")


def split_sql_values(values_block: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    in_str = False
    i = 0
    while i < len(values_block):
        ch = values_block[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                if i + 1 < len(values_block) and values_block[i + 1] == "'":
                    buf.append(values_block[i + 1])
                    i += 2
                    continue
                in_str = False
            i += 1
            continue

        if ch == "'":
            in_str = True
            buf.append(ch)
        elif ch == ",":
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1

    if buf:
        parts.append("".join(buf).strip())
    return parts


def parse_sql_string(token: str) -> str:
    t = token.strip()
    if t.upper() == "NULL":
        return ""
    if len(t) >= 2 and t[0] == "'" and t[-1] == "'":
        return t[1:-1].replace("''", "'")
    return t


def normalize_num(token: str) -> str:
    t = token.strip()
    if t.upper() == "NULL" or t == "":
        return ""
    if len(t) >= 2 and t[0] == "'" and t[-1] == "'":
        t = t[1:-1]
    try:
        f = float(t)
        if f.is_integer():
            return str(int(f))
        return t
    except ValueError:
        return t


TS_RE = re.compile(r"^\{ts\s+'([^']+)'\}$", flags=re.IGNORECASE)


def parse_datetime(token: str) -> str:
    t = token.strip()
    if t.upper() == "NULL":
        return ""
    m = TS_RE.match(t)
    if m:
        return m.group(1)
    return parse_sql_string(t)


def parse_insert_record(values: list[str]) -> list[str]:
    # Formatos aceitos no bruto:
    # 6 colunas: RCL_PAC, RCL_COD, RCL_DTHR, RCL_OSM, RCL_MED, RCL_TXT
    # 5 colunas: RCL_PAC, RCL_COD, RCL_DTHR, RCL_MED, RCL_TXT
    if len(values) >= 6:
        rcl_med_idx = 4
        rcl_txt_idx = 5
    elif len(values) == 5:
        rcl_med_idx = 3
        rcl_txt_idx = 4
    else:
        raise RuntimeError(f"INSERT com numero inesperado de colunas: {len(values)}")

    rcl_pac = normalize_num(values[0])
    rcl_cod = parse_sql_string(values[1])
    rcl_dthr = parse_datetime(values[2])
    rcl_med = normalize_num(values[rcl_med_idx])
    rcl_txt = parse_sql_string(values[rcl_txt_idx])
    rcl_laudo_rtf = ""

    return [rcl_pac, rcl_cod, rcl_dthr, rcl_med, rcl_txt, rcl_laudo_rtf]


def process_sql_file(path: Path, writer: csv.writer, requested_encoding: str) -> int:
    enc = detect_encoding(path, requested_encoding)
    text = path.read_text(encoding=enc, errors="replace")

    inserts = 0
    pos = 0
    while True:
        idx = text.find("INSERT INTO", pos)
        if idx == -1:
            break

        values_idx = text.find("VALUES", idx)
        if values_idx == -1:
            break

        open_idx = text.find("(", values_idx)
        if open_idx == -1:
            break

        block, close_idx = extract_parenthesized(text, open_idx)
        raw_values = split_sql_values(block)
        row = parse_insert_record(raw_values)
        writer.writerow(row)
        inserts += 1
        pos = close_idx + 1

    return inserts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Gera data/initial/initial.csv a partir dos arquivos SQL brutos."
    )
    p.add_argument("--raw-dir", default="data/raw", help="Diretorio com arquivos .SQL")
    p.add_argument("--pattern", default="*.SQL", help="Padrao de arquivo bruto")
    p.add_argument("--out", default="data/initial/initial.csv", help="CSV inicial consolidado")
    p.add_argument("--input-encoding", default="auto", help="Encoding dos SQLs (auto, utf-8, cp1252, latin-1)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_dir = Path(args.raw_dir)
    if not raw_dir.exists():
        raise RuntimeError(f"Diretorio nao encontrado: {raw_dir}")

    files = sorted(raw_dir.glob(args.pattern))
    if not files:
        raise RuntimeError(f"Nenhum arquivo encontrado em {raw_dir} com padrao {args.pattern}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    with out_path.open("w", encoding="utf-8", newline="") as wf:
        writer = csv.writer(wf)
        writer.writerow(["rcl_pac", "rcl_cod", "rcl_dthr", "rcl_med", "rcl_txt", "rcl_laudo_rtf"])

        for file_path in files:
            count = process_sql_file(file_path, writer, args.input_encoding)
            total_rows += count
            print(f"[OK] {file_path.name}: {count} registros")

    print("Consolidado gerado com sucesso.")
    print(f"Arquivo: {out_path}")
    print(f"Total de registros: {total_rows}")


if __name__ == "__main__":
    main()
