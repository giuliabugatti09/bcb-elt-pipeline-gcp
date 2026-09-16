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
BASE_URL_ULTIMOS = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados/ultimos/{n}"

# Diretório raiz de dados brutos (relativo à raiz do projeto)
RAW_DATA_DIR = Path("data/raw")

# Alguns provedores (incluindo o BCB) bloqueiam requisições cujo User-Agent
# identifica claramente uma biblioteca automatizada (ex: "python-requests/2.32.3"),
# retornando 406 Not Acceptable mesmo que os parâmetros estejam corretos.
# Simulamos um cliente de navegador comum para contornar esse bloqueio.
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


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
            response = requests.get(
                url, params=params, headers=DEFAULT_HEADERS, timeout=30
            )
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
    ultimos_n: Optional[int] = None,
) -> list[dict]:
    """
    Extrai uma série temporal específica da API do BCB.

    IMPORTANTE (descoberto na prática): a API do BCB rejeita com 406
    Not Acceptable requisições sem intervalo de datas quando a série
    tem muito histórico — ela não retorna "tudo" por padrão como outras
    APIs REST costumam fazer. Por isso este método SEMPRE exige um dos
    dois modos abaixo, nunca uma chamada "sem filtro".

    Modos de uso (mutuamente exclusivos):
        1. ultimos_n: busca os N valores mais recentes.
           Uso típico: carga incremental do dia a dia (Airflow rodando
           diariamente só precisa do último valor, não do histórico).

        2. data_inicial + data_final: busca um intervalo específico.
           Uso típico: carga inicial (backfill) do histórico.
           Restrição da própria API: o intervalo não pode ultrapassar
           10 anos — por isso validamos isso aqui antes de gastar uma
           chamada de rede.

    Args:
        series_name: chave em SERIES_BCB (ex: 'dolar_ptax_venda')
        data_inicial: formato 'DD/MM/AAAA'. Usado apenas no modo backfill.
        data_final: formato 'DD/MM/AAAA'. Usado apenas no modo backfill.
        ultimos_n: quantidade de valores mais recentes. Usado no modo incremental.

    Returns:
        Lista de dicts com formato [{"data": "15/09/2026", "valor": "5.42"}, ...]
    """
    if series_name not in SERIES_BCB:
        raise ValueError(
            f"Série '{series_name}' não configurada. "
            f"Opções disponíveis: {list(SERIES_BCB.keys())}"
        )

    codigo_serie = SERIES_BCB[series_name]

    if ultimos_n is not None:
        # Modo incremental: últimos N valores
        url = BASE_URL_ULTIMOS.format(codigo=codigo_serie, n=ultimos_n)
        params = {"formato": "json"}

    elif data_inicial and data_final:
        # Modo backfill: intervalo de datas
        _validar_intervalo_datas(data_inicial, data_final)
        url = BASE_URL.format(codigo=codigo_serie)
        params = {
            "formato": "json",
            "dataInicial": data_inicial,
            "dataFinal": data_final,
        }

    else:
        raise ValueError(
            "É obrigatório informar 'ultimos_n' OU o par "
            "'data_inicial'+'data_final'. A API do BCB não aceita "
            "requisições sem intervalo definido (retorna 406)."
        )

    dados = _fetch_with_retry(url, params)
    logger.info(f"Série '{series_name}' extraída com sucesso: {len(dados)} registros.")
    return dados


def _validar_intervalo_datas(data_inicial: str, data_final: str) -> None:
    """
    Valida que o intervalo entre as datas não ultrapassa 10 anos,
    conforme documentado pela API do BCB. Falhar rápido aqui evita
    gastar uma chamada de rede que sabemos que vai ser rejeitada.
    """
    dt_inicial = datetime.strptime(data_inicial, "%d/%m/%Y")
    dt_final = datetime.strptime(data_final, "%d/%m/%Y")

    if dt_final <= dt_inicial:
        raise ValueError("data_final deve ser posterior a data_inicial.")

    dias_diferenca = (dt_final - dt_inicial).days
    if dias_diferenca > 365 * 10:
        raise ValueError(
            f"Intervalo de {dias_diferenca} dias excede o limite de 10 anos "
            f"imposto pela API do BCB. Divida a extração em blocos menores."
        )


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
    ultimos_n: Optional[int] = None,
) -> Path:
    """
    Função de entrada principal: extrai e salva uma série.
    É essa função que o Airflow vai chamar na DAG (Dia 5/6).
    """
    logger.info(f"Iniciando extração da série: {series_name}")
    dados = extract_series(series_name, data_inicial, data_final, ultimos_n)
    file_path = save_raw_json(series_name, dados)
    logger.info(f"Extração concluída: {series_name}")
    return file_path


if __name__ == "__main__":
    # Execução manual para teste local.
    # Usamos o modo incremental (últimos 30 valores) por padrão — é o
    # modo que a DAG do Airflow vai usar no dia a dia. O backfill
    # completo do histórico será feito separadamente no Dia 4.
    for series in SERIES_BCB:
        run_extraction(series, ultimos_n=20)