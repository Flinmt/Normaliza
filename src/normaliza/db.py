from __future__ import annotations

from dataclasses import dataclass

from .config import DbConfig


@dataclass(frozen=True)
class AtrField:
    code: str
    num: str
    label: str
    field_type: str


def _connect(config: DbConfig, database: str):
    try:
        import pyodbc
    except ImportError as exc:
        raise RuntimeError("pyodbc não instalado. Execute: pip install -r requirements.txt") from exc

    installed = set(pyodbc.drivers())
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    candidates = [driver for driver in preferred if driver in installed]

    if not candidates:
        raise RuntimeError(
            "Nenhum driver ODBC SQL Server encontrado. "
            f"Drivers instalados: {', '.join(sorted(installed)) or '(nenhum)'}"
        )

    last_error: Exception | None = None
    for driver in candidates:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={config.host},{config.port};"
            f"DATABASE={database};"
            f"UID={config.user};PWD={config.password};"
            "Encrypt=no;TrustServerCertificate=yes;"
        )
        try:
            return pyodbc.connect(conn_str, timeout=30)
        except Exception as exc:  # pragma: no cover
            last_error = exc

    raise RuntimeError(
        "Falha ao conectar no SQL Server com drivers detectados. "
        f"Erro final: {last_error}"
    )


def load_atr_lookup(config: DbConfig, database: str) -> dict[tuple[str, str], AtrField]:
    conn = _connect(config, database)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ATR_DSC, ATR_NUM, ATR_ROT, ATR_TIPO
            FROM ATR
            WHERE ATR_DSC IS NOT NULL AND ATR_NUM IS NOT NULL
            """
        )

        lookup: dict[tuple[str, str], AtrField] = {}
        for atr_dsc, atr_num, atr_rot, atr_tipo in cur.fetchall():
            code = str(atr_dsc).strip()
            num = str(atr_num).strip()
            lookup[(code, num)] = AtrField(
                code=code,
                num=num,
                label="" if atr_rot is None else str(atr_rot).strip(),
                field_type="" if atr_tipo is None else str(atr_tipo).strip(),
            )
        return lookup
    finally:
        conn.close()


def load_psv_professional_lookup(config: DbConfig, database: str) -> dict[str, str]:
    conn = _connect(config, database)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT PSV_COD, intProfissionalId
            FROM PSV
            WHERE intProfissionalId IS NOT NULL
              AND PSV_COD IS NOT NULL
            """
        )

        lookup: dict[str, str] = {}
        for psv_cod, profissional_id in cur.fetchall():
            psv_key = str(psv_cod).strip()
            if not psv_key:
                continue
            lookup[psv_key] = str(profissional_id).strip()
        return lookup
    finally:
        conn.close()
