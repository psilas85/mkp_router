# =========================================================
# 📦 src/mkp_preprocessing/scripts/excluir_geo_invalidos_por_input.py
# =========================================================

import argparse
from loguru import logger
from psycopg2.extras import RealDictCursor
from database.db_connection import get_connection


def excluir_invalidos(tenant_id: int, input_id: str):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # 1️⃣ Buscar PDVs inválidos
                cur.execute(
                    """
                    SELECT id, endereco_cache_key
                    FROM pdvs
                    WHERE tenant_id = %s
                      AND input_id = %s
                      AND geo_validacao_status = 'fora_cidade';
                    """,
                    (tenant_id, input_id)
                )
                rows = cur.fetchall()

                if not rows:
                    logger.warning("⚠️ Nenhum PDV inválido encontrado.")
                    return

                pdv_ids = [r["id"] for r in rows]
                cache_keys = list({
                    r["endereco_cache_key"]
                    for r in rows
                    if r["endereco_cache_key"]
                })

                logger.info(f"🗑️ PDVs inválidos: {len(pdv_ids)}")
                logger.info(f"🔥 Cache keys a remover (GLOBAL): {len(cache_keys)}")

                # 2️⃣ Excluir PDVs
                cur.execute(
                    """
                    DELETE FROM pdvs
                    WHERE id = ANY(%s);
                    """,
                    (pdv_ids,)
                )
                logger.success(f"✅ PDVs excluídos: {cur.rowcount}")

                # 3️⃣ Excluir cache GLOBAL (SEM CONDIÇÃO)
                if cache_keys:
                    cur.execute(
                        """
                        DELETE FROM enderecos_cache
                        WHERE endereco = ANY(%s);
                        """,
                        (cache_keys,)
                    )
                    logger.success(f"🔥 Cache removido globalmente: {cur.rowcount}")

    except Exception as e:
        logger.error(f"❌ Erro na exclusão: {e}", exc_info=True)
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Excluir PDVs fora da cidade + cache global"
    )
    parser.add_argument("--tenant_id", type=int, required=True)
    parser.add_argument("--input_id", type=str, required=True)
    args = parser.parse_args()

    logger.info(
        f"🚨 Exclusão GEO iniciada | tenant={args.tenant_id} | input={args.input_id}"
    )

    excluir_invalidos(args.tenant_id, args.input_id)

    logger.success("🏁 Exclusão GEO finalizada.")


if __name__ == "__main__":
    main()
