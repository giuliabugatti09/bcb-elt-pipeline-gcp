"""
Loader responsável por ler um JSON já armazenado no Supabase Storage
(Data Lake) e carregá-lo na tabela raw.bcb_series do Postgres (Data
Warehouse) via UPSERT idempotente.

Fluxo completo do pipeline até aqui:
    API BCB -> Airflow -> Supabase Storage (raw JSON) -> Postgres (raw.bcb_series)

Este módulo é o último elo dessa cadeia. Ele não conhece a API do BCB
nem sabe como o arquivo chegou no Storage — só sabe ler um JSON de lá
e inserir no formato certo no banco.
"""
import json
import os
from datetime import datetime
from decimal import Decimal
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from src.loaders.supabase_storage_loader import (
    build_incremental_remote_path,
    download_file,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

load_dotenv()

SUPABASE_DB_CONNECTION_STRING = os.environ.get("SUPABASE_DB_CONNECTION_STRING")


class PostgresLoadError(Exception):
    """Erro customizado para falhas de carga no Postgres."""
    pass


def _parse_registro(registro: dict) -> tuple:
    """
    Converte um registro bruto da API do BCB (formato
    {"data": "16/09/2026", "valor": "5.42"}) para os tipos Python
    corretos (date, Decimal) antes de inserir no banco.
    """
    data_referencia = datetime.strptime(registro["data"], "%d/%m/%Y").date()
    valor = Decimal(registro["valor"])
    return data_referencia, valor


def load_series_to_postgres(series_name: str, execution_date_iso: str) -> int:
    """
    Baixa o JSON incremental de uma série do Supabase Storage e faz
    UPSERT dos registros na tabela raw.bcb_series.

    Args:
        series_name: nome da série (ex: 'dolar_ptax_venda')
        execution_date_iso: data no formato 'YYYY-MM-DD', usada para
            localizar o arquivo correto no Storage

    Returns:
        Quantidade de registros processados (inseridos ou atualizados)
    """
    if not SUPABASE_DB_CONNECTION_STRING:
        raise PostgresLoadError(
            "SUPABASE_DB_CONNECTION_STRING não configurada. "
            "Confira o .env ou as variáveis do container."
        )

    remote_path = build_incremental_remote_path(series_name, execution_date_iso)

    logger.info(f"Baixando '{remote_path}' do Storage para carga no Postgres")
    conteudo_bytes = download_file(remote_path)
    payload = json.loads(conteudo_bytes)

    registros = payload["data"]
    extraction_timestamp = payload["extraction_timestamp"]

    linhas = [
        (
            series_name,
            *_parse_registro(registro),
            extraction_timestamp,
            remote_path,
        )
        for registro in registros
    ]

    logger.info(
        f"Preparando UPSERT de {len(linhas)} registro(s) da série "
        f"'{series_name}' na tabela raw.bcb_series"
    )

    conn = None
    try:
        conn = psycopg2.connect(SUPABASE_DB_CONNECTION_STRING)
        with conn:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    """
                    INSERT INTO raw.bcb_series
                        (series_name, data_referencia, valor,
                         extraction_timestamp, source_file)
                    VALUES %s
                    ON CONFLICT (series_name, data_referencia)
                    DO UPDATE SET
                        valor = EXCLUDED.valor,
                        extraction_timestamp = EXCLUDED.extraction_timestamp,
                        source_file = EXCLUDED.source_file
                    """,
                    linhas,
                )
        logger.info(
            f"Carga concluída: {len(linhas)} registro(s) da série "
            f"'{series_name}' processados em raw.bcb_series"
        )
        return len(linhas)

    except psycopg2.Error as e:
        raise PostgresLoadError(
            f"Falha ao carregar dados da série '{series_name}' no Postgres: {e}"
        ) from e
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    # Teste manual local: carrega os dados de hoje de uma série
    # específica. Rode a extração + upload antes, para garantir que o
    # arquivo exista no Storage.
    hoje = datetime.now().date().isoformat()
    load_series_to_postgres("selic_diaria", hoje)