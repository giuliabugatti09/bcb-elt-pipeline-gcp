"""
Loader responsável por subir arquivos brutos (JSON) do disco local para
o Supabase Storage (nosso Data Lake na nuvem).

Segue os mesmos princípios já aplicados no extrator (Dia 3):
- Retry com backoff exponencial para falhas transitórias de rede.
- Upload idempotente (upsert): subir o mesmo arquivo duas vezes apenas
  sobrescreve, nunca duplica.
- Responsabilidade única: este módulo só sabe fazer upload. Ele não
  sabe nada sobre como os dados foram extraídos.
"""
import os
import time
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Carrega variáveis do .env quando rodando localmente (fora do Airflow).
# Dentro do container, essas variáveis já vêm injetadas via
# docker-compose (environment), então load_dotenv() simplesmente não
# encontra nada para sobrescrever e não tem efeito — seguro nos dois casos.
load_dotenv()

SUPABASE_PROJECT_URL = os.environ.get("SUPABASE_PROJECT_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_STORAGE_BUCKET = os.environ.get("SUPABASE_STORAGE_BUCKET", "bcb-raw-data")


class SupabaseUploadError(Exception):
    """Erro customizado para falhas de upload ao Supabase Storage."""
    pass


def _validar_configuracao() -> None:
    """
    Falha rápido e com mensagem clara se as credenciais não estiverem
    configuradas, em vez de deixar a requisição HTTP falhar de forma
    confusa mais adiante.
    """
    faltando = [
        nome
        for nome, valor in [
            ("SUPABASE_PROJECT_URL", SUPABASE_PROJECT_URL),
            ("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY),
        ]
        if not valor
    ]
    if faltando:
        raise SupabaseUploadError(
            f"Variáveis de ambiente ausentes: {', '.join(faltando)}. "
            f"Confira o arquivo .env ou as variáveis do container."
        )


def upload_file(
    local_file_path: Path,
    remote_path: str,
    max_retries: int = 3,
    initial_backoff_seconds: float = 2.0,
) -> str:
    """
    Faz upload de um arquivo local para o Supabase Storage, com retry
    e backoff exponencial (mesmo padrão do extrator BCB).

    Args:
        local_file_path: caminho do arquivo local a ser enviado
        remote_path: caminho de destino dentro do bucket, ex:
            "dolar_ptax_venda/incremental/dolar_ptax_venda_2026-09-16.json"
        max_retries: tentativas antes de desistir
        initial_backoff_seconds: espera inicial entre tentativas

    Returns:
        O remote_path enviado (útil para log/confirmação)
    """
    _validar_configuracao()

    if not local_file_path.exists():
        raise FileNotFoundError(f"Arquivo local não encontrado: {local_file_path}")

    url = f"{SUPABASE_PROJECT_URL}/storage/v1/object/{SUPABASE_STORAGE_BUCKET}/{remote_path}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
        # upsert=true: reenviar o mesmo path sobrescreve em vez de
        # retornar erro de "arquivo já existe" — é isso que garante
        # a idempotência do upload.
        "x-upsert": "true",
    }

    file_bytes = local_file_path.read_bytes()
    last_exception: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Tentativa {attempt}/{max_retries} — upload de "
                f"'{local_file_path.name}' para '{remote_path}'"
            )
            response = requests.post(
                url, headers=headers, data=file_bytes, timeout=30
            )
            response.raise_for_status()
            logger.info(f"Upload concluído: {remote_path}")
            return remote_path

        except requests.exceptions.RequestException as e:
            last_exception = e
            wait_time = initial_backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                f"Falha na tentativa {attempt}/{max_retries}: {e}. "
                f"Aguardando {wait_time:.1f}s antes de tentar novamente."
            )
            if attempt < max_retries:
                time.sleep(wait_time)

    raise SupabaseUploadError(
        f"Falha ao subir '{local_file_path}' após {max_retries} tentativas. "
        f"Último erro: {last_exception}"
    )


def download_file(
    remote_path: str,
    max_retries: int = 3,
    initial_backoff_seconds: float = 2.0,
) -> bytes:
    """
    Baixa um arquivo do Supabase Storage e retorna seu conteúdo em bytes.

    Usado pela camada de carga (Dia 11) para ler de volta os JSONs que
    o extrator subiu, sem depender do arquivo ainda existir no disco
    local — a Storage é a fonte de verdade da camada Data Lake, não o
    disco efêmero do worker que rodou a extração.

    Args:
        remote_path: caminho do arquivo dentro do bucket

    Returns:
        Conteúdo do arquivo em bytes
    """
    _validar_configuracao()

    url = f"{SUPABASE_PROJECT_URL}/storage/v1/object/{SUPABASE_STORAGE_BUCKET}/{remote_path}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
    }

    last_exception: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Tentativa {attempt}/{max_retries} — download de '{remote_path}'"
            )
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            logger.info(f"Download concluído: {remote_path}")
            return response.content

        except requests.exceptions.RequestException as e:
            last_exception = e
            wait_time = initial_backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                f"Falha na tentativa {attempt}/{max_retries}: {e}. "
                f"Aguardando {wait_time:.1f}s antes de tentar novamente."
            )
            if attempt < max_retries:
                time.sleep(wait_time)

    raise SupabaseUploadError(
        f"Falha ao baixar '{remote_path}' após {max_retries} tentativas. "
        f"Último erro: {last_exception}"
    )


def build_incremental_remote_path(series_name: str, execution_date_iso: str) -> str:
    """
    Monta o caminho padronizado de um arquivo incremental dentro do
    bucket, dado o nome da série e a data (YYYY-MM-DD). Centralizado
    aqui para que extractors, loaders e a camada Postgres (Dia 11)
    sempre concordem sobre essa convenção de nomenclatura.
    """
    filename = f"{series_name}_{execution_date_iso}.json"
    return f"{series_name}/incremental/{filename}"


def upload_latest_incremental(series_name: str, execution_date_iso: str) -> str:
    """
    Localiza e sobe o arquivo de extração incremental de uma série para
    uma data específica. Espelha exatamente a estrutura de pastas local
    (data/raw/{serie}/incremental/) dentro do bucket, para manter a
    correspondência 1:1 entre o que está local e o que está na nuvem.

    Args:
        series_name: nome da série (ex: 'dolar_ptax_venda')
        execution_date_iso: data no formato 'YYYY-MM-DD'

    Returns:
        O remote_path enviado
    """
    filename = f"{series_name}_{execution_date_iso}.json"
    local_path = Path("data/raw") / series_name / "incremental" / filename
    remote_path = build_incremental_remote_path(series_name, execution_date_iso)

    return upload_file(local_path, remote_path)


if __name__ == "__main__":
    # Teste manual local: sobe o arquivo de hoje de uma série específica.
    # Rode a extração (bcp_extractor) antes, para garantir que o arquivo
    # local exista.
    from datetime import datetime

    hoje = datetime.now().date().isoformat()
    upload_latest_incremental("selic_diaria", hoje)