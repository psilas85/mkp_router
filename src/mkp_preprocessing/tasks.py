#mkp_router/src/mkp_preprocessing/tasks.py

import logging
import argparse

from mkp_preprocessing.mkp_jobs import processar_mkp
from mkp_preprocessing.infrastructure.queue_factory import (
    fila_preprocessing,
    LONG_TIMEOUT,
)

# ============================================================
# 🔧 Logger
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("mkp_tasks")


# ============================================================
# 🚀 CLI → Enfileira job de preprocessing
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Enfileirar job de MKP Preprocessing")

    parser.add_argument("tenant_id", type=int, help="ID do tenant")
    parser.add_argument("arquivo", help="Caminho do arquivo XLSX/CSV")
    parser.add_argument("descricao", help="Descrição do job")
    parser.add_argument(
        "--usar_google",
        action="store_true",
        help="Habilita fallback Google Geocoding"
    )

    args = parser.parse_args()

    tenant_id = args.tenant_id
    file_path = args.arquivo
    descricao = args.descricao
    usar_google = args.usar_google

    # --------------------------------------------------------
    # Enfileira na fila CORRETA
    # --------------------------------------------------------

    queue = fila_preprocessing()

    job = queue.enqueue(
        processar_mkp,
        tenant_id,
        file_path,
        descricao,
        meta={"usar_google": usar_google},
        timeout=LONG_TIMEOUT
    )


    logger.info(
        f"🚀 Job enfileirado com sucesso | "
        f"job_id={job.id} | "
        f"tenant={tenant_id} | "
        f"usar_google={usar_google}"
    )


if __name__ == "__main__":
    main()
