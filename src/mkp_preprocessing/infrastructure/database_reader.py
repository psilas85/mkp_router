#mkp_router/src/mkp_preprocessing/infrastructure/database_reader.py

import os
import time
import logging
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from contextlib import closing
from typing import Optional, Dict, List, Tuple, Any
from functools import wraps
from mkp_preprocessing.domain.address_normalizer import normalize_for_cache


# ============================================================
# ⚙️ POOL DE CONEXÕES (thread-safe)
# ============================================================

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "sales_routing_db")),
    "user": os.getenv("DB_USER", os.getenv("POSTGRES_USER", "postgres")),
    "password": os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")),
    "host": os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "sales_router_db")),
    "port": os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")),
}

POOL = ThreadedConnectionPool(
    minconn=1,
    maxconn=20,  # suficiente e seguro para RQ + API
    **DB_PARAMS
)


logging.info("🔌 ThreadedConnectionPool inicializado para PDV Preprocessing.")


# ============================================================
# 🔁 Decorator de retry automático
# ============================================================

def retry_on_failure(max_retries=3, delay=0.5, backoff=2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tentativa = 0
            while tentativa < max_retries:
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    tentativa += 1
                    logging.warning(
                        f"⚠️ Erro de conexão ({func.__name__}) tentativa "
                        f"{tentativa}/{max_retries}: {e}"
                    )
                    time.sleep(delay * (backoff ** (tentativa - 1)))
                except Exception as e:
                    logging.error(
                        f"❌ Erro inesperado em {func.__name__}: {e}",
                        exc_info=True
                    )
                    break
            logging.error(f"🚨 Falha após {max_retries} tentativas em {func.__name__}")
            return None
        return wrapper
    return decorator


# ============================================================
# 📚 DatabaseReader com POOL seguro
# ============================================================

class DatabaseReader:
    """
    Leitura segura no PostgreSQL com pool de conexões.
    Todas as operações são threadsafe.
    """

    def __init__(self):
        pass  # não guarda conexão fixa


    # ============================================================
    # 🔍 Buscar endereço no cache (enderecos_cache)
    # ============================================================

    @retry_on_failure()
    def buscar_localizacao(self, endereco: str) -> Optional[Tuple[float, float]]:
        if not endereco:
            return None

        endereco_norm = normalize_for_cache(endereco)

        conn = POOL.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT lat, lon
                    FROM enderecos_cache
                    WHERE endereco = %s
                    LIMIT 1;
                    """,
                    (endereco_norm,),
                )
                row = cur.fetchone()
                return (row["lat"], row["lon"]) if row else None

        except Exception as e:
            logging.warning(f"⚠️ [CACHE_DB] Falha ao buscar '{endereco_norm}': {e}")
            return None

        finally:
            POOL.putconn(conn)

    
    # ============================================================
    # 🧠 Consulta PDV existente por tenant e CNPJ
    # ============================================================
    @retry_on_failure()
    def buscar_pdv_por_cnpj(self, tenant_id: int, cnpj: str) -> Optional[Dict[str, Any]]:
        conn = POOL.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, cnpj, cidade, uf, pdv_lat, pdv_lon
                    FROM pdvs
                    WHERE tenant_id = %s AND cnpj = %s
                    LIMIT 1;
                    """,
                    (tenant_id, cnpj),
                )
                return cur.fetchone()
        except Exception as e:
            logging.warning(f"⚠️ [PDV_DB] Erro ao buscar PDV existente ({cnpj}): {e}")
            return None
        finally:
            POOL.putconn(conn)

    # ============================================================
    # 📋 Carrega todos os PDVs de um tenant
    # ============================================================
    @retry_on_failure()
    def listar_pdvs_por_tenant(self, tenant_id: int) -> pd.DataFrame:
        conn = POOL.getconn()
        try:
            query = """
                SELECT *
                FROM pdvs
                WHERE tenant_id = %s
                ORDER BY cidade, bairro;
            """
            df = pd.read_sql_query(query, conn, params=(tenant_id,))
            df = df.replace([float("inf"), float("-inf")], pd.NA)
            df = df.where(pd.notnull(df), None)
            return df
        except Exception as e:
            logging.warning(f"⚠️ [PDV_DB] Erro ao listar PDVs (tenant={tenant_id}): {e}")
            return pd.DataFrame()
        finally:
            POOL.putconn(conn)


    # ============================================================
    # 🧾 Busca CNPJs existentes (respeitando input_id)
    # ============================================================
    @retry_on_failure()
    def buscar_cnpjs_existentes(self, tenant_id: int, input_id: Optional[str] = None) -> List[str]:
        """
        Retorna todos os CNPJs já existentes no banco para o tenant,
        opcionalmente filtrando por input_id.
        """
        if input_id:
            query = """
                SELECT cnpj 
                FROM pdvs 
                WHERE tenant_id = %s AND input_id = %s;
            """
            params = (tenant_id, input_id)
        else:
            query = """
                SELECT cnpj 
                FROM pdvs 
                WHERE tenant_id = %s;
            """
            params = (tenant_id,)

        conn = POOL.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                return [row["cnpj"] for row in rows]

        except Exception as e:
            logging.warning(
                f"⚠️ [PDV_DB] Erro ao buscar CNPJs existentes (tenant={tenant_id}, input_id={input_id}): {e}"
            )
            return []

        finally:
            POOL.putconn(conn)



    # ============================================================
    # 🔍 Buscar múltiplos endereços no cache
    # ============================================================

    @retry_on_failure()
    def buscar_enderecos_cache(self, enderecos: List[str]) -> Dict[str, Tuple[float, float]]:
        if not enderecos:
            return {}

        end_norm = [normalize_for_cache(e) for e in enderecos if e]

        conn = POOL.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT endereco, lat, lon
                    FROM enderecos_cache
                    WHERE endereco = ANY(%s);
                    """,
                    (end_norm,),
                )

                return {
                    row["endereco"]: (row["lat"], row["lon"])
                    for row in cur.fetchall()
                }

        except Exception as e:
            logging.warning(f"⚠️ [CACHE_DB] Erro batch: {e}")
            return {}

        finally:
            POOL.putconn(conn)
     

    # ============================================================
    # 📋 Listar últimos 10 jobs (para /jobs/ultimos)
    # ============================================================
    @retry_on_failure()
    def listar_ultimos_jobs(self, tenant_id: int, limite: int = 10) -> pd.DataFrame:
        conn = POOL.getconn()
        try:
            query = """
                SELECT 
                    id, tenant_id, input_id, descricao, arquivo, status,
                    total_processados, validos, invalidos, arquivo_invalidos,
                    mensagem, criado_em, inseridos, sobrescritos
                FROM historico_pdv_jobs
                WHERE tenant_id = %s
                ORDER BY criado_em DESC
                LIMIT %s;
            """
            df = pd.read_sql_query(query, conn, params=(tenant_id, limite))
            df = df.replace([float("inf"), float("-inf")], pd.NA)
            df = df.where(pd.notnull(df), None)
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao listar últimos jobs: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            POOL.putconn(conn)

    # ============================================================
    # 📋 Listar jobs (para /jobs) — máximo 100
    # ============================================================
    @retry_on_failure()
    def listar_jobs(self, tenant_id: int, limite: int = 100) -> pd.DataFrame:
        conn = POOL.getconn()
        try:
            query = """
                SELECT 
                    id, tenant_id, input_id, descricao, arquivo, status,
                    total_processados, validos, invalidos, arquivo_invalidos,
                    mensagem, criado_em, inseridos, sobrescritos
                FROM historico_pdv_jobs
                WHERE tenant_id = %s
                ORDER BY criado_em DESC
                LIMIT %s;
            """
            df = pd.read_sql_query(query, conn, params=(tenant_id, limite))
            df = df.replace([float("inf"), float("-inf")], pd.NA)
            df = df.where(pd.notnull(df), None)
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao listar jobs: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            POOL.putconn(conn)

    # ============================================================
    # 🔍 Filtrar jobs por data + descrição (para /jobs/filtrar)
    # ============================================================
    @retry_on_failure()
    def filtrar_jobs(
        self,
        tenant_id: int,
        data_inicio: str = None,
        data_fim: str = None,
        descricao: str = None,
        limite: int = 10
    ) -> pd.DataFrame:

        filtros = ["tenant_id = %s"]
        params = [tenant_id]

        # converte dd/mm/aaaa → yyyy-mm-dd
        def normalizar_data(data: str):
            if "/" in data:
                d, m, a = data.split("/")
                return f"{a}-{m}-{d}"
            return data

        if data_inicio:
            data_inicio = normalizar_data(data_inicio)
            filtros.append("DATE(criado_em) >= %s")
            params.append(data_inicio)

        if data_fim:
            data_fim = normalizar_data(data_fim)
            filtros.append("DATE(criado_em) <= %s")
            params.append(data_fim)

        if descricao:
            filtros.append("descricao ILIKE %s")
            params.append(f"%{descricao}%")

        where = " AND ".join(filtros)

        sql = f"""
            SELECT 
                id, tenant_id, input_id, descricao, arquivo, status,
                total_processados, validos, invalidos, arquivo_invalidos,
                mensagem, criado_em, inseridos, sobrescritos
            FROM historico_pdv_jobs
            WHERE {where}
            ORDER BY criado_em DESC
            LIMIT %s;
        """

        params.append(limite)

        conn = POOL.getconn()
        try:
            df = pd.read_sql_query(sql, conn, params=tuple(params))
            df = df.replace([float("inf"), float("-inf")], pd.NA)
            df = df.where(pd.notnull(df), None)
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao filtrar jobs: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            POOL.putconn(conn)

    @retry_on_failure()
    def listar_pdvs_sem_geo_validacao(
        self,
        tenant_id: int,
        limite: int = 1000
    ) -> List[Dict[str, Any]]:
        conn = POOL.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        cidade,
                        uf,
                        pdv_lat,
                        pdv_lon
                    FROM pdvs
                    WHERE tenant_id = %s
                    AND pdv_lat IS NOT NULL
                    AND pdv_lon IS NOT NULL
                    AND geo_validacao_status IS NULL
                    LIMIT %s;
                    """,
                    (tenant_id, limite)
                )
                return cur.fetchall()
        except Exception as e:
            logging.error(f"❌ Erro ao listar PDVs sem geo_validacao: {e}", exc_info=True)
            return []
        finally:
            POOL.putconn(conn)

    @retry_on_failure()
    def contar_pdvs_por_geo_status(self, tenant_id: int) -> Dict[str, int]:
        conn = POOL.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT geo_validacao_status, count(*) as total
                    FROM pdvs
                    WHERE tenant_id = %s
                    GROUP BY geo_validacao_status;
                    """,
                    (tenant_id,)
                )
                return {
                    row["geo_validacao_status"]: row["total"]
                    for row in cur.fetchall()
                }
        finally:
            POOL.putconn(conn)

    @retry_on_failure()
    def listar_pdvs_por_input(
        self,
        tenant_id: int,
        input_id: str
    ) -> List[Dict[str, Any]]:
        conn = POOL.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        tenant_id,
                        input_id,
                        cidade,
                        uf,
                        pdv_lat,
                        pdv_lon
                    FROM pdvs
                    WHERE tenant_id = %s
                    AND input_id = %s
                    AND pdv_lat IS NOT NULL
                    AND pdv_lon IS NOT NULL;
                    """,
                    (tenant_id, input_id)
                )
                return cur.fetchall()
        finally:
            POOL.putconn(conn)

