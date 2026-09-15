"""
Utilitário centralizado de logging.

Por que ter isso separado?
Em vez de cada script configurar seu próprio logging (e todo mundo
formatando diferente), centralizamos aqui. Assim, quando o projeto
crescer (Airflow, dbt, etc), todos os logs seguem o mesmo padrão,
o que facilita muito debugar em produção.
"""
import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Cria (ou reutiliza) um logger configurado.

    Args:
        name: geralmente __name__ do módulo que está chamando

    Returns:
        Logger configurado com handler para stdout
    """
    logger = logging.getLogger(name)

    # Evita adicionar handlers duplicados se a função for chamada
    # mais de uma vez para o mesmo logger (comum em notebooks/testes)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger