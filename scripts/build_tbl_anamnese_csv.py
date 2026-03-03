#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import re
from pathlib import Path


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
    # Keeps SQL-friendly style and trims microseconds to milliseconds.
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


def _compose_html(rcl_cod: str, rcl_dthr: str, rendered: str) -> str:
    title = _title_from_rcl_cod(rcl_cod)
    dt = html.escape((rcl_dthr or "").strip())
    content = (rendered or "").strip()

    # If already structured HTML from transformer, remove old <h2> and inject <h3>/<h4>.
    if "<ul>" in content and "<li>" in content:
        content_wo_h2 = re.sub(r"^\s*<h2>.*?</h2>\s*", "", content, count=1, flags=re.S)
        return f"<h3>{html.escape(title)}</h3> <h4>{dt}</h4> {content_wo_h2}".strip()

    # Plain text: preserve text and wrap minimally.
    safe_text = html.escape(content).replace("\n", "<br>")
    return f"<h3>{html.escape(title)}</h3> <h4>{dt}</h4> <p>{safe_text}</p>"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Gera CSV normalizado para carga na tabela tblAnamnese."
    )
    p.add_argument("--in", dest="input_csv", default="output/rcl_transformado.csv")
    p.add_argument("--out", dest="output_csv", default="output/tblAnamnese_import.csv")
    p.add_argument("--start-id", type=int, required=True, help="ID inicial manual para intAnamneseId")
    p.add_argument("--int-usuario-id", type=int, default=1)
    p.add_argument("--int-empresa-id", type=int, default=1)
    p.add_argument("--int-especialidade-medica-id", type=int, default=0)
    p.add_argument("--bol-nao-compartilhar", default="0")
    p.add_argument("--bol-json", default="0")
    p.add_argument("--str-json-default", default="")
    p.add_argument("--bol-triagem-mode", choices=["auto", "null", "zero"], default="auto")
    return p.parse_args()


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

    current_id = args.start_id
    total = 0

    with source.open("r", encoding="utf-8", errors="replace", newline="") as rf, target.open(
        "w", encoding="utf-8", newline=""
    ) as wf:
        reader = csv.DictReader(rf)
        writer = csv.DictWriter(wf, fieldnames=out_fields)
        writer.writeheader()

        for row in reader:
            total += 1
            rcl_cod = (row.get("rcl_cod") or "").strip()
            rcl_dthr = (row.get("rcl_dthr") or "").strip()
            rendered = row.get("rcl_txt_render") or row.get("rcl_txt_html") or row.get("rcl_txt") or ""

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
