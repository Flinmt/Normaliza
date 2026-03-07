#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"


def ask(prompt: str, default: str) -> str:
    value = input(f"{prompt} [{default}]: ").strip()
    return value or default


def run_cmd(args: list[str]) -> int:
    print("\nExecutando:")
    print(" ".join(args))
    print("-" * 72)
    result = subprocess.run(args, cwd=str(ROOT))
    print("-" * 72)
    return int(result.returncode)


def run_or_stop(args: list[str], step_name: str) -> bool:
    code = run_cmd(args)
    if code != 0:
        print(f"Etapa falhou: {step_name} (codigo {code}).")
        return False
    return True


def flow_carga_inicial() -> None:
    print("\n=== Fluxo Carga Inicial ===")
    env_path = ask("Arquivo .env", ".env")
    database = ask("Banco de destino", "BIODATA_HVISAO")
    map_db = ask("Banco do mapa de cliente", "REPOSITORIO_HVISAO")
    raw_pattern = ask("Padrao dos arquivos brutos", "*.SQL")
    start_id = ask("intAnamneseId inicial (manual)", "1")

    with tempfile.TemporaryDirectory(prefix="normaliza_initial_", dir=str(ROOT / "output")) as tmp_dir:
        tmp_path = Path(tmp_dir)
        initial_csv = tmp_path / "initial_tmp.csv"
        transformed_csv = tmp_path / "rcl_transformado_tmp.csv"
        summary_json = tmp_path / "transform_summary_tmp.json"

        if not run_or_stop([
            sys.executable,
            str(SCRIPTS_DIR / "raw_sql_to_initial_csv.py"),
            "--raw-dir", "data/raw",
            "--pattern", raw_pattern,
            "--out", str(initial_csv),
            "--input-encoding", "auto",
        ], "Gerar CSV consolidado temporario"):
            return

        if not run_or_stop([
            sys.executable,
            str(SCRIPTS_DIR / "transform_rcl.py"),
            "--csv", str(initial_csv),
            "--env", env_path,
            "--database", database,
            "--client-map-database", map_db,
            "--out", str(transformed_csv),
            "--summary", str(summary_json),
        ], "Transformar CSV temporario"):
            return

        if not run_or_stop([
            sys.executable,
            str(SCRIPTS_DIR / "transformed_csv_to_tbl_anamnese_csv.py"),
            "--in", str(transformed_csv),
            "--out", "output/tblAnamnese_import.csv",
            "--start-id", start_id,
        ], "Gerar CSV tblAnamnese"):
            return

    if not run_or_stop([
        sys.executable,
        str(SCRIPTS_DIR / "preview_csv.py"),
        "--csv", "output/tblAnamnese_import.csv",
        "--out-dir", "output/preview_tblAnamnese_import",
        "--encoding", "auto",
        "--ensure-anamnese-mix",
        "--mix-per-type", "20",
    ], "Preview do CSV de importacao"):
        return

    print("\nCarga inicial preparada com sucesso (sem importação).")
    print("Audite os arquivos gerados:")
    print("- output/tblAnamnese_import.csv")
    print("- output/preview_tblAnamnese_import/head_preview.csv")
    print("- output/preview_tblAnamnese_import/sample_preview.csv")
    print("- output/preview_tblAnamnese_import/summary_preview.json")
    print("Depois, use o menu Avançado para Backup + Importação da carga inicial.")


def flow_carga_incremental() -> None:
    print("\n=== Fluxo Carga Incremental ===")
    env_path = ask("Arquivo .env", ".env")
    database = ask("Banco de destino", "BIODATA_HVISAO")
    map_db = ask("Banco do mapa de cliente", "REPOSITORIO_HVISAO")
    raw_pattern = ask("Padrao dos arquivos incrementais", "*.SQL")

    with tempfile.TemporaryDirectory(prefix="normaliza_incremental_", dir=str(ROOT / "output")) as tmp_dir:
        tmp_path = Path(tmp_dir)
        incremental_csv = tmp_path / "incremental_tmp.csv"
        transformed_csv = tmp_path / "rcl_transformado_incremental_tmp.csv"
        summary_json = tmp_path / "transform_summary_incremental_tmp.json"

        if not run_or_stop([
            sys.executable,
            str(SCRIPTS_DIR / "raw_sql_to_initial_csv.py"),
            "--raw-dir", "data/incremental",
            "--pattern", raw_pattern,
            "--out", str(incremental_csv),
            "--input-encoding", "auto",
        ], "Gerar CSV incremental temporario"):
            return

        if not run_or_stop([
            sys.executable,
            str(SCRIPTS_DIR / "transform_rcl.py"),
            "--csv", str(incremental_csv),
            "--env", env_path,
            "--database", database,
            "--client-map-database", map_db,
            "--out", str(transformed_csv),
            "--summary", str(summary_json),
        ], "Transformar incremental temporario"):
            return

        if not run_or_stop([
            sys.executable,
            str(SCRIPTS_DIR / "transformed_csv_to_tbl_anamnese_csv.py"),
            "--in", str(transformed_csv),
            "--out", "output/tblAnamnese_import_incremental.csv",
            "--auto-start-id",
            "--env", env_path,
            "--database", database,
        ], "Gerar CSV tblAnamnese incremental"):
            return

    if not run_or_stop([
        sys.executable,
        str(SCRIPTS_DIR / "preview_csv.py"),
        "--csv", "output/tblAnamnese_import_incremental.csv",
        "--out-dir", "output/preview_tblAnamnese_import_incremental",
        "--encoding", "auto",
        "--ensure-anamnese-mix",
        "--mix-per-type", "20",
    ], "Preview do CSV incremental de importacao"):
        return

    print("\nCarga incremental preparada com sucesso (sem importação).")
    print("Audite os arquivos gerados:")
    print("- output/tblAnamnese_import_incremental.csv")
    print("- output/preview_tblAnamnese_import_incremental/head_preview.csv")
    print("- output/preview_tblAnamnese_import_incremental/sample_preview.csv")
    print("- output/preview_tblAnamnese_import_incremental/summary_preview.json")
    print("Depois, use o menu Avançado para Backup + Importação incremental.")


def run_import(csv_path: str, default_batch: str = "200") -> None:
    env_path = ask("Arquivo .env", ".env")
    database = ask("Banco de destino", "BIODATA_HVISAO")
    batch_size = ask("Tamanho do lote", default_batch)
    run_cmd([
        sys.executable,
        str(SCRIPTS_DIR / "import_tbl_anamnese_csv_batches.py"),
        "--csv", csv_path,
        "--env", env_path,
        "--database", database,
        "--batch-size", batch_size,
    ])


def advanced_menu() -> None:
    while True:
        print("\n=== Avancado ===")
        print("1) Backup tblAnamnese")
        print("2) Importar carga inicial (output/tblAnamnese_import.csv)")
        print("3) Importar carga incremental (output/tblAnamnese_import_incremental.csv)")
        print("4) Remover dados da tblAnamnese em lotes (perigoso)")
        print("5) Voltar")
        choice = input("Escolha uma opcao (1-5): ").strip()

        if choice == "1":
            env_path = ask("Arquivo .env", ".env")
            database = ask("Banco de destino", "BIODATA_HVISAO")
            run_cmd([
                sys.executable,
                str(SCRIPTS_DIR / "backup_tbl_anamnese.py"),
                "--env", env_path,
                "--database", database,
            ])
        elif choice == "2":
            run_import("output/tblAnamnese_import.csv")
        elif choice == "3":
            run_import("output/tblAnamnese_import_incremental.csv")
        elif choice == "4":
            env_path = ask("Arquivo .env", ".env")
            database = ask("Banco de destino", "BIODATA_HVISAO")
            batch_size = ask("Tamanho do lote", "2000")
            confirm = ask("Executar com --yes? (sim/nao)", "nao").lower()
            cmd = [
                sys.executable,
                str(SCRIPTS_DIR / "purge_tbl_anamnese_batches.py"),
                "--env", env_path,
                "--database", database,
                "--batch-size", batch_size,
            ]
            if confirm in {"sim", "s", "yes", "y"}:
                cmd.append("--yes")
            run_cmd(cmd)
        elif choice == "5":
            return
        else:
            print("Opcao invalida.")


def main() -> None:
    while True:
        print("\n=== Normaliza | Simples ===")
        print("1) Carga Inicial")
        print("2) Carga Incremental")
        print("3) Avancado")
        print("4) Sair")
        choice = input("Escolha uma opcao (1-4): ").strip()

        if choice == "1":
            flow_carga_inicial()
        elif choice == "2":
            flow_carga_incremental()
        elif choice == "3":
            advanced_menu()
        elif choice == "4":
            print("Encerrado.")
            return
        else:
            print("Opcao invalida.")


if __name__ == "__main__":
    main()

