from __future__ import annotations

import csv
import sys
from pathlib import Path

from .decoder import decode_structured_text


def set_max_csv_field_size() -> None:
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


def transform_csv(
    csv_path: str,
    output_path: str,
    atr_lookup,
    psv_lookup,
    client_lookup,
) -> dict[str, int]:
    set_max_csv_field_size()

    source = Path(csv_path)
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    stats = {
        "rows_total": 0,
        "rows_structured": 0,
        "rows_plain": 0,
        "rows_med_mapped": 0,
        "rows_med_unmapped": 0,
        "rows_client_mapped": 0,
        "rows_client_unmapped": 0,
    }

    with source.open("r", encoding="utf-8", errors="replace", newline="") as rf, target.open(
        "w", encoding="utf-8", newline=""
    ) as wf:
        reader = csv.DictReader(rf)
        if not reader.fieldnames:
            raise RuntimeError("CSV sem cabeçalho")

        for required in ("rcl_cod", "rcl_txt"):
            if required not in reader.fieldnames:
                raise RuntimeError(f"CSV sem coluna obrigatória: {required}")

        output_fields = list(reader.fieldnames) + [
            "rcl_pac_original",
            "rcl_med_original",
            "rcl_txt_html",
            "rcl_txt_original",
            "rcl_txt_render",
        ]
        writer = csv.DictWriter(wf, fieldnames=output_fields)
        writer.writeheader()

        for row in reader:
            stats["rows_total"] += 1
            rcl_cod = (row.get("rcl_cod") or "").strip()
            original = row.get("rcl_txt") or ""
            original_med = (row.get("rcl_med") or "").strip()
            original_pac = (row.get("rcl_pac") or "").strip()

            html_render = decode_structured_text(original, rcl_cod, atr_lookup)
            rendered = html_render if html_render else original

            if html_render:
                stats["rows_structured"] += 1
            else:
                stats["rows_plain"] += 1

            out_row = dict(row)
            out_row["rcl_pac_original"] = original_pac
            if original_pac and original_pac in client_lookup:
                out_row["rcl_pac"] = client_lookup[original_pac]
                stats["rows_client_mapped"] += 1
            else:
                stats["rows_client_unmapped"] += 1

            out_row["rcl_med_original"] = original_med
            if original_med and original_med in psv_lookup:
                out_row["rcl_med"] = psv_lookup[original_med]
                stats["rows_med_mapped"] += 1
            else:
                stats["rows_med_unmapped"] += 1
            out_row["rcl_txt_html"] = html_render
            out_row["rcl_txt_original"] = original
            out_row["rcl_txt_render"] = rendered
            writer.writerow(out_row)

    return stats
