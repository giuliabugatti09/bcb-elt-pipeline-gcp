"""
Extrator de séries temporais da API SGS (Sistema Gerenciador de Séries
Temporais) do Banco Central do Brasil.

Documentação da API: https://dadosabertos.bcb.gov.br/dataset/

Princípios aplicados aqui:
- Idempotência: salva por data de execução, reprocessar não duplica.
- Resiliência: retry com backoff exponencial em falhas de rede.
- Separação de responsabilidades: só extrai e salva raw, não transforma nada.
"""
import json
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Séries do BCB que vamos usar no projeto.
# Código de referência: https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do
SERIES_BCB = {
    "dolar_ptax_venda": 1,
    "ipca_mensal": 433,
    "selic_diaria": 11,
}

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

# Diretório raiz de dados brutos (relativo à raiz do projeto)
RAW_DATA_DIR = Path("data/raw")


class BCBExtractionError(Exception):
    """Erro customizado para falhas na extração da API do BCB."""
    pass


def _fetch_with_retry(
    url: str,
    params: dict,
    max_retries: int = 3,
    initial_backoff_seconds: float = 2.0,
) -> list[dict]:
    """
    Faz a requisição HTTP com retry e backoff exponencial.

    Por que backoff exponencial? Se a API estiver com instabilidade
    momentânea, esperar um pouco mais a cada tentativa (2s, 4s, 8s...)
    dá tempo dela se recuperar, em vez de martelar requisições em
    sequência rápida (o que pode até piorar o problema do lado da API).
    """
    last_exception: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentativa {attempt}/{max_retries} — GET {url}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # levanta exceção se status >= 400
            return response.json()

        except requests.exceptions.RequestException as e:
            last_exception = e
            wait_time = initial_backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                f"Falha na tentativa {attempt}/{max_retries}: {e}. "
                f"Aguardando {wait_time:.1f}s antes de tentar novamente."
            )
            if attempt < max_retries:
                time.sleep(wait_time)

    # Se chegou aqui, esgotou todas as tentativas
    raise BCBExtractionError(
        f"Falha ao extrair dados após {max_retries} tentativas. "
        f"Último erro: {last_exception}"
    )


def extract_series(
    series_name: str,
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
) -> list[dict]:
    """
    Extrai uma série temporal específica da API do BCB.

    Args:
        series_name: chave em SERIES_BCB (ex: 'dolar_ptax_venda')
        data_inicial: formato 'DD/MM/AAAA'. Se None, busca desde o início.
        data_final: formato 'DD/MM/AAAA'. Se None, busca até hoje.

    Returns:
        Lista de dicts com formato [{"data": "15/09/2026", "valor": "5.42"}, ...]
    """
    if series_name not in SERIES_BCB:
        raise ValueError(
            f"Série '{series_name}' não configurada. "
            f"Opções disponíveis: {list(SERIES_BCB.keys())}"
        )

    codigo_serie = SERIES_BCB[series_name]
    url = BASE_URL.format(codigo=codigo_serie)

    params = {"formato": "json"}
    if data_inicial:
        params["dataInicial"] = data_inicial
    if data_final:
        params["dataFinal"] = data_final

    dados = _fetch_with_retry(url, params)
    logger.info(f"Série '{series_name}' extraída com sucesso: {len(dados)} registros.")
    return dados


def save_raw_json(
    series_name: str,
    data: list[dict],
    execution_date: Optional[date] = None,
) -> Path:
    """
    Salva os dados brutos em JSON, particionados por série e data de execução.

    Estrutura resultante (idempotente — reprocessar o mesmo dia sobrescreve):
        data/raw/dolar_ptax_venda/dolar_ptax_venda_2026-09-15.json

    Args:
        series_name: nome da série (usado como subpasta)
        data: dados retornados por extract_series()
        execution_date: data de execução. Se None, usa hoje.

    Returns:
        Path do arquivo salvo
    """
    if execution_date is None:
        execution_date = datetime.now().date()

    series_dir = RAW_DATA_DIR / series_name
    series_dir.mkdir(parents=True, exist_ok=True)

    file_path = series_dir / f"{series_name}_{execution_date.isoformat()}.json"

    payload = {
        "series_name": series_name,
        "extraction_timestamp": datetime.now().isoformat(),
        "record_count": len(data),
        "data": data,
    }

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"Dados salvos em: {file_path}")
    return file_path


def run_extraction(
    series_name: str,
    data_inicial: Optional[str] = None,
    data_final: Optional[str] = None,
) -> Path:
    """
    Função de entrada principal: extrai e salva uma série.
    É essa função que o Airflow vai chamar na DAG (Dia 5/6).
    """
    logger.info(f"Iniciando extração da série: {series_name}")
    dados = extract_series(series_name, data_inicial, data_final)
    file_path = save_raw_json(series_name, dados)
    logger.info(f"Extração concluída: {series_name}")
    return file_path


if __name__ == "__main__":
    # Execução manual para teste local — vamos rodar isso hoje
    # para validar que a extração funciona antes de conectar ao Airflow.
    for series in SERIES_BCB:
        run_extraction(series)