#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
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


def run_transform() -> None:
    csv_path = ask("CSV de origem", "rcl.csv")
    env_path = ask("Arquivo .env", ".env")
    database = ask("Banco", "BIODATA_HVISAO")
    client_map_database = ask("Banco do mapa de cliente", "REPOSITORIO_HVISAO")
    out_path = ask("CSV de saida", "output/rcl_transformado.csv")
    summary = ask("Resumo JSON", "output/transform_summary.json")

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "transform_rcl.py"),
        "--csv",
        csv_path,
        "--env",
        env_path,
        "--database",
        database,
        "--client-map-database",
        client_map_database,
        "--out",
        out_path,
        "--summary",
        summary,
    ]
    code = run_cmd(cmd)
    print(f"Finalizado com codigo: {code}\n")


def run_preview(default_csv: str, default_out_dir: str) -> None:
    csv_path = ask("CSV para preview", default_csv)
    out_dir = ask("Pasta de saida do preview", default_out_dir)
    chunksize = ask("Chunksize", "150000")
    head = ask("Linhas de head", "200")
    sample = ask("Tamanho da amostra", "50000")
    columns = ask(
        "Colunas da amostra (separadas por virgula)",
        "rcl_pac,rcl_cod,rcl_dthr,rcl_med,rcl_txt,rcl_txt_html,rcl_txt_render",
    )

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "preview_csv_pandas.py"),
        "--csv",
        csv_path,
        "--out-dir",
        out_dir,
        "--chunksize",
        chunksize,
        "--head",
        head,
        "--sample",
        sample,
        "--columns",
        columns,
    ]
    code = run_cmd(cmd)
    print(f"Finalizado com codigo: {code}\n")


def run_tbl_anamnese_export() -> None:
    in_csv = ask("CSV transformado de entrada", "output/rcl_transformado.csv")
    out_csv = ask("CSV de saida para tblAnamnese", "output/tblAnamnese_import.csv")
    start_id = ask("intAnamneseId inicial (manual)", "1")
    int_usuario = ask("intUsuarioId", "1")
    int_empresa = ask("intEmpresaId", "1")
    int_especialidade = ask("intEspecialidadeMedicaId", "0")
    bol_nao_comp = ask("bolNaoCompartilhar", "0")
    bol_json = ask("bolJson", "0")
    bol_triagem_mode = ask("bolTriagem mode (auto/null/zero)", "auto")

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "build_tbl_anamnese_csv.py"),
        "--in",
        in_csv,
        "--out",
        out_csv,
        "--start-id",
        start_id,
        "--int-usuario-id",
        int_usuario,
        "--int-empresa-id",
        int_empresa,
        "--int-especialidade-medica-id",
        int_especialidade,
        "--bol-nao-compartilhar",
        bol_nao_comp,
        "--bol-json",
        bol_json,
        "--bol-triagem-mode",
        bol_triagem_mode,
    ]
    code = run_cmd(cmd)
    print(f"Finalizado com codigo: {code}\n")


def run_tbl_anamnese_import_batches() -> None:
    csv_path = ask("CSV para importar", "output/tblAnamnese_import.csv")
    env_path = ask("Arquivo .env", ".env")
    database = ask("Banco de destino", "BIODATA_HVISAO")
    batch_size = ask("Tamanho do lote", "2000")

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "import_tbl_anamnese_batches.py"),
        "--csv",
        csv_path,
        "--env",
        env_path,
        "--database",
        database,
        "--batch-size",
        batch_size,
    ]
    code = run_cmd(cmd)
    print(f"Finalizado com codigo: {code}\n")


def run_tbl_anamnese_delete_batches() -> None:
    env_path = ask("Arquivo .env", ".env")
    database = ask("Banco de destino", "BIODATA_HVISAO")
    batch_size = ask("Tamanho do lote de exclusao", "2000")
    confirm = ask("Executar com --yes? (sim/nao)", "nao").lower()

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "delete_tbl_anamnese_batches.py"),
        "--env",
        env_path,
        "--database",
        database,
        "--batch-size",
        batch_size,
    ]
    if confirm in {"sim", "s", "yes", "y"}:
        cmd.append("--yes")

    code = run_cmd(cmd)
    print(f"Finalizado com codigo: {code}\n")


def main() -> None:
    while True:
        print("=== Normaliza | Menu ===")
        print("1) Transformar CSV legado")
        print("2) Gerar preview do CSV original")
        print("3) Gerar preview do CSV transformado")
        print("4) Transformar e gerar preview do transformado")
        print("5) Gerar CSV para tblAnamnese")
        print("6) Importar CSV em lotes para tblAnamnese")
        print("7) Remover dados da tblAnamnese em lotes (perigoso)")
        print("8) Sair")
        choice = input("Escolha uma opcao (1-8): ").strip()

        if choice == "1":
            run_transform()
        elif choice == "2":
            run_preview("rcl.csv", "output/preview")
        elif choice == "3":
            run_preview("output/rcl_transformado.csv", "output/preview_transformado")
        elif choice == "4":
            run_transform()
            run_preview("output/rcl_transformado.csv", "output/preview_transformado")
        elif choice == "5":
            run_tbl_anamnese_export()
        elif choice == "6":
            run_tbl_anamnese_import_batches()
        elif choice == "7":
            run_tbl_anamnese_delete_batches()
        elif choice == "8":
            print("Encerrado.")
            return
        else:
            print("Opcao invalida.\n")


if __name__ == "__main__":
    main()
