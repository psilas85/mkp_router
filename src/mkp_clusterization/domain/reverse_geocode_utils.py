# =========================================================
# 📦 mkp_router/src/mkp_clusterization/domain/reverse_geocode_utils.py
# =========================================================

import os
import requests
import pandas as pd
from loguru import logger
from database.db_connection import get_connection

# =========================================================
# 🌍 Config (blindada)
# =========================================================
_BASE_URL = os.getenv("NOMINATIM_LOCAL_URL", "http://nominatim:8080").rstrip("/")

# garante /reverse mesmo que o .env não tenha
if not _BASE_URL.endswith("/reverse"):
    NOMINATIM_URL = f"{_BASE_URL}/reverse"
else:
    NOMINATIM_URL = _BASE_URL

HEADERS = {"User-Agent": "mkp-router/1.0 (admin@mkprouter.local)"}


# =========================================================
# 🧭 Batch enrich (1x por centro)
# =========================================================
def enrich_centros_reverse(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe DF com centro_lat / centro_lon
    Retorna DF com:
      - Endereco centro (logradouro + bairro)
      - Cidade centro
      - UF centro
      - CEP centro
    """
    results: list[dict] = []

    centros = (
        df[["centro_lat", "centro_lon"]]
        .dropna()
        .drop_duplicates()
        .to_dict("records")
    )

    for c in centros:
        geo = reverse_geocode_centro(float(c["centro_lat"]), float(c["centro_lon"])) or {}

        results.append(
            {
                "centro_lat": c["centro_lat"],
                "centro_lon": c["centro_lon"],
                "Endereco centro": geo.get("endereco"),
                "Cidade centro": geo.get("cidade"),
                "UF centro": geo.get("uf"),
                "CEP centro": geo.get("cep"),
            }
        )

    return pd.DataFrame(results)


# =========================================================
# 📍 Reverse geocode unitário + cache
# =========================================================
def reverse_geocode_centro(lat: float, lon: float) -> dict | None:
    """
    Reverse geocode via Nominatim local.
    Retorna SOMENTE:
      - endereco: logradouro + bairro
      - cidade
      - uf
      - cep
    """

    conn = get_connection()

    try:
        # =====================================================
        # 🔎 Cache por lat/lon
        # =====================================================
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT endereco, cidade, uf, cep
                FROM centros_geocode_cache
                WHERE lat = %s AND lon = %s
                LIMIT 1;
                """,
                (lat, lon),
            )
            row = cur.fetchone()
            if row:
                return {
                    "endereco": row[0],
                    "cidade": row[1],
                    "uf": row[2],
                    "cep": row[3],
                }

        # =====================================================
        # 🌍 Reverse Nominatim
        # =====================================================
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "addressdetails": 1,
        }

        resp = requests.get(
            NOMINATIM_URL,
            params=params,
            headers=HEADERS,
            timeout=10,
        )

        if resp.status_code != 200:
            logger.warning(
                f"⚠️ Reverse geocode falhou | url={resp.url} | status={resp.status_code} "
                f"| ct={resp.headers.get('content-type')} | body={(resp.text or '')[:200]}"
            )
            return None

        data = resp.json()
        addr = data.get("address", {}) or {}

        # =====================================================
        # 🏷️ Normalização limpa (SEM display_name)
        # =====================================================
        logradouro = (
            addr.get("road")
            or addr.get("pedestrian")
            or addr.get("footway")
            or addr.get("highway")
        )

        bairro = (
            addr.get("suburb")
            or addr.get("neighbourhood")
            or addr.get("quarter")
        )

        cidade = (
            addr.get("city")
            or addr.get("town")
            or addr.get("municipality")
            or addr.get("village")
        )

        uf = addr.get("state_code") or addr.get("state")
        cep = addr.get("postcode")

        # =====================================================
        # 🧹 Montagem + blindagem contra lixo/POI
        # =====================================================
        endereco_parts: list[str] = []
        if logradouro:
            endereco_parts.append(str(logradouro).strip())
        if bairro:
            endereco_parts.append(str(bairro).strip())

        endereco = ", ".join([p for p in endereco_parts if p])

        # Se por algum motivo entrar lixo (POI/regiões), cai para logradouro/bairro
        if endereco:
            lixo = (
                "hospital",
                "região",
                "brasil",
                "metropolitana",
                "geográfica",
                "unidade",
                "zona",
            )
            if any(x in endereco.lower() for x in lixo):
                endereco = (logradouro or bairro or endereco)

        # =====================================================
        # 💾 Cache
        # =====================================================
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO centros_geocode_cache
                        (lat, lon, endereco, cidade, uf, cep)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (lat, lon) DO NOTHING;
                    """,
                    (lat, lon, endereco, cidade, uf, cep),
                )

        return {
            "endereco": endereco,
            "cidade": cidade,
            "uf": uf,
            "cep": cep,
        }

    except Exception as e:
        logger.error(f"❌ Erro reverse geocode centro: {e}")
        return None

    finally:
        conn.close()
