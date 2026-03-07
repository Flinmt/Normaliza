#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Gera artefatos leves para visualizar CSV grande usando pandas em chunks."
    )
    p.add_argument("--csv", default="data/initial/initial.csv", help="CSV de origem")
    p.add_argument("--out-dir", default="output/preview", help="Pasta de saída")
    p.add_argument("--chunksize", type=int, default=150_000, help="Linhas por chunk")
    p.add_argument("--head", type=int, default=200, help="Quantidade de linhas no head")
    p.add_argument("--sample", type=int, default=50_000, help="Amostra aleatória total")
    p.add_argument("--seed", type=int, default=42, help="Seed da amostra")
    p.add_argument(
        "--columns",
        default="rcl_pac,rcl_cod,rcl_dthr,rcl_med,rcl_txt",
        help="Colunas para a amostra (separadas por vírgula)",
    )
    p.add_argument(
        "--encoding",
        default="auto",
        help="Encoding do CSV (ex.: auto, utf-8, cp1252, latin-1)",
    )
    p.add_argument(
        "--ensure-anamnese-mix",
        action="store_true",
        help="Garante exemplos de anamnese estruturada (<ul><li>) e texto plano no sample.",
    )
    p.add_argument(
        "--mix-per-type",
        type=int,
        default=10,
        help="Quantidade minima por tipo (estruturada/plana) quando --ensure-anamnese-mix estiver ativo.",
    )
    p.add_argument(
        "--anamnese-column",
        default="strAnamnese",
        help="Coluna para detectar anamnese estruturada/plana.",
    )
    return p


def resolve_input_encoding(csv_path: Path, requested: str) -> str:
    if requested.lower() != "auto":
        return requested

    with csv_path.open("rb") as rf:
        sample = rf.read(512_000)
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _safe_value_counts(series: pd.Series, topn: int = 20) -> dict[str, int]:
    s = series.fillna("<NULL>").astype(str)
    vc = s.value_counts(dropna=False).head(topn)
    return {str(k): int(v) for k, v in vc.items()}


def main() -> None:
    args = build_parser().parse_args()

    csv_path = Path(args.csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_encoding = resolve_input_encoding(csv_path, args.encoding)

    selected_columns = [c.strip() for c in args.columns.split(",") if c.strip()]

    # Métricas acumuladas
    total_rows = 0
    rows_with_structured = 0
    rcl_cod_counter: Counter[str] = Counter()
    null_counts: Counter[str] = Counter()

    # Amostragem incremental por chunk
    sampled_parts: list[pd.DataFrame] = []
    head_df: pd.DataFrame | None = None
    structured_examples: list[pd.DataFrame] = []
    plain_examples: list[pd.DataFrame] = []

    # Lê tudo como string para evitar inferência custosa e inconsistências
    reader = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        chunksize=args.chunksize,
        low_memory=True,
        encoding=source_encoding,
        on_bad_lines="skip",
    )

    for chunk in reader:
        total_rows += len(chunk)

        if head_df is None:
            head_df = chunk.head(args.head).copy()

        # Contagem de nulos/vazios
        for col in chunk.columns:
            null_counts[col] += int((chunk[col].isna() | (chunk[col] == "")).sum())

        if "rcl_txt" in chunk.columns:
            rows_with_structured += int(chunk["rcl_txt"].str.contains(r"@#\d+@\d+", regex=True, na=False).sum())

        if "rcl_cod" in chunk.columns:
            rcl_cod_counter.update(chunk["rcl_cod"].fillna("<NULL>").astype(str).tolist())

        # Amostragem por chunk (proporcional)
        if args.sample > 0:
            frac = min(1.0, args.sample / max(total_rows, 1))
            part = chunk.sample(frac=frac, random_state=args.seed)
            sampled_parts.append(part)

        if args.ensure_anamnese_mix and args.anamnese_column in chunk.columns:
            col = chunk[args.anamnese_column].fillna("").astype(str)
            structured_mask = col.str.contains("<ul>", regex=False) & col.str.contains("<li>", regex=False)
            plain_mask = ~structured_mask

            need_structured = max(args.mix_per_type - sum(len(x) for x in structured_examples), 0)
            if need_structured > 0:
                pick = chunk.loc[structured_mask].head(need_structured)
                if not pick.empty:
                    structured_examples.append(pick)

            need_plain = max(args.mix_per_type - sum(len(x) for x in plain_examples), 0)
            if need_plain > 0:
                pick = chunk.loc[plain_mask].head(need_plain)
                if not pick.empty:
                    plain_examples.append(pick)

    if total_rows == 0:
        raise RuntimeError("CSV vazio ou inválido.")

    # Consolida amostra final
    if sampled_parts:
        sampled_df = pd.concat(sampled_parts, ignore_index=True)
        if len(sampled_df) > args.sample:
            sampled_df = sampled_df.sample(n=args.sample, random_state=args.seed)
    else:
        sampled_df = pd.DataFrame()

    if args.ensure_anamnese_mix:
        extras: list[pd.DataFrame] = []
        if structured_examples:
            extras.append(pd.concat(structured_examples, ignore_index=True))
        if plain_examples:
            extras.append(pd.concat(plain_examples, ignore_index=True))
        if extras:
            sampled_df = pd.concat([sampled_df] + extras, ignore_index=True).drop_duplicates()

    # Reduz para colunas desejadas, se existirem
    existing_cols = [c for c in selected_columns if c in sampled_df.columns]
    if existing_cols:
        sampled_df = sampled_df[existing_cols]

    # Arquivos de saída para visualização
    head_path = out_dir / "head_preview.csv"
    sample_path = out_dir / "sample_preview.csv"
    summary_path = out_dir / "summary_preview.json"

    if head_df is not None:
        head_df.to_csv(head_path, index=False, encoding="utf-8")

    sampled_df.to_csv(sample_path, index=False, encoding="utf-8")

    summary = {
        "csv": str(csv_path.resolve()),
        "input_encoding": source_encoding,
        "total_rows": int(total_rows),
        "columns": list(head_df.columns) if head_df is not None else [],
        "rows_with_structured_pattern": int(rows_with_structured),
        "rows_without_structured_pattern": int(total_rows - rows_with_structured),
        "top_20_rcl_cod": rcl_cod_counter.most_common(20),
        "null_or_empty_per_column": {k: int(v) for k, v in null_counts.items()},
        "sample_rows": int(len(sampled_df)),
    }

    # Value counts úteis na amostra
    if "rcl_cod" in sampled_df.columns and len(sampled_df):
        summary["sample_top_rcl_cod"] = _safe_value_counts(sampled_df["rcl_cod"], topn=20)

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Concluído.")
    print(f"Head: {head_path}")
    print(f"Amostra: {sample_path}")
    print(f"Resumo: {summary_path}")


if __name__ == "__main__":
    main()
