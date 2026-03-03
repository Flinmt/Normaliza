from __future__ import annotations

import html
import re
from dataclasses import dataclass

from .db import AtrField

# Token estruturado: @#<codigo>@<campo><marcador><valor>
TOKEN_RE = re.compile(r"@#(\d+)@(\d+)([A-Z%&U])(.*?)(?=@#\d+@\d+[A-Z%&U]|$)", re.DOTALL)


@dataclass(frozen=True)
class DecodedEntry:
    code: str
    field_num: str
    marker: str
    value: str
    label: str


@dataclass(frozen=True)
class DecodedBlock:
    title: str
    entries: list[DecodedEntry]


def normalize_title(raw: str) -> str:
    base = (raw or "").strip()
    lower = base.lower()

    if lower == "triagem":
        title = "TRI@GEM"
    elif base.startswith("101010"):
        title = "CONSULTA"
    else:
        title = base or "SEM_TITULO"

    return f"{title} - Migrado"


def decode_structured_text(
    text: str,
    rcl_cod: str,
    atr_lookup: dict[tuple[str, str], AtrField],
) -> str:
    matches = list(TOKEN_RE.finditer(text or ""))
    if not matches:
        return ""

    entries: list[DecodedEntry] = []
    for m in matches:
        code, field_num, marker, value = m.group(1), m.group(2), m.group(3), m.group(4)
        value = value.strip()
        field = atr_lookup.get((code, field_num))
        label = field.label if field and field.label else f"Campo {field_num}"
        entries.append(
            DecodedEntry(
                code=code,
                field_num=field_num,
                marker=marker,
                value=value,
                label=label,
            )
        )

    title = normalize_title(rcl_cod)
    lines: list[str] = [f"<h2>{html.escape(title)}</h2>", "<ul>"]

    for e in entries:
        shown = e.value if e.value else "(sem valor)"
        safe_label = html.escape(e.label)
        safe_value = html.escape(shown).replace("\n", "<br>")
        lines.append(f"<li><strong>{safe_label}:</strong> {safe_value}</li>")

    lines.append("</ul>")
    return "\n".join(lines)
