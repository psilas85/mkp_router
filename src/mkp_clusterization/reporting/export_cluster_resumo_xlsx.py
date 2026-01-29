# =========================================================
# 📦 src/mkp_clusterization/reporting/export_cluster_resumo_xlsx.py
# =========================================================

import os
import argparse
import pandas as pd
from loguru import logger

from database.db_connection import get_connection
from mkp_clusterization.domain.haversine_utils import haversine
from mkp_clusterization.domain.reverse_geocode_utils import enrich_centros_reverse

VELOCIDADE_MEDIA_KMH = 35.0  # alinhado com clusterização


# =========================================================
# 🧮 Métricas geométricas reais por cluster (PDV → centro)
# =========================================================
def calcular_metricas_por_cluster(df_pdvs: pd.DataFrame) -> pd.DataFrame:
    registros = []

    for cluster_id, g in df_pdvs.groupby("cluster_id"):
        distancias = []

        for _, row in g.iterrows():
            d = haversine(
                (row["pdv_lat"], row["pdv_lon"]),
                (row["centro_lat"], row["centro_lon"]),
            )
            distancias.append(d)

        if not distancias:
            continue

        dist_media = sum(distancias) / len(distancias)
        dist_max = max(distancias)

        tempo_medio = (dist_media / VELOCIDADE_MEDIA_KMH) * 60
        tempo_max = (dist_max / VELOCIDADE_MEDIA_KMH) * 60

        registros.append(
            {
                "cluster_id": cluster_id,
                "dist_media_km": round(dist_media, 2),
                "dist_max_km": round(dist_max, 2),
                "tempo_medio_min": round(tempo_medio, 1),
                "tempo_max_min": round(tempo_max, 1),
            }
        )

    return pd.DataFrame(registros)


# =========================================================
# 📤 Exportação principal
# =========================================================
def exportar_cluster_resumo(tenant_id: int, clusterization_id: str):
    logger.info(
        f"📊 Exportando resumo de clusters | tenant={tenant_id} | clusterization_id={clusterization_id}"
    )

    conn = get_connection()

    # =========================================================
    # 🔍 Run mais recente
    # =========================================================
    run_df = pd.read_sql_query(
        f"""
        SELECT id AS run_id
        FROM cluster_run
        WHERE tenant_id = {tenant_id}
          AND clusterization_id = '{clusterization_id}'
        ORDER BY criado_em DESC
        LIMIT 1;
        """,
        conn,
    )

    if run_df.empty:
        conn.close()
        raise ValueError("❌ Nenhum run encontrado")

    run_id = int(run_df.iloc[0]["run_id"])

    # =========================================================
    # 📊 Resumo base (metrics = fonte de verdade)
    # =========================================================
    df = pd.read_sql_query(
        f"""
        SELECT
            cs.id AS cluster_id,
            cs.cluster_label,
            cs.n_pdvs AS enderecos,

            (cs.metrics->>'tempo_medio_min')::float     AS tempo_medio_min,
            (cs.metrics->>'tempo_max_min')::float       AS tempo_max_min,
            (cs.metrics->>'distancia_media_km')::float  AS distancia_media_km,
            (cs.metrics->>'dist_max_km')::float          AS dist_max_km,

            cs.centro_lat,
            cs.centro_lon,

            cc.endereco AS endereco_centro,
            cc.cnpj     AS cnpj_centro,
            cc.bandeira,
            cc.origem_geocode                -- 👈 ESSENCIAL

        FROM cluster_setor cs
        JOIN cluster_centro cc
        ON cc.id = cs.centro_id
        WHERE cs.tenant_id = {tenant_id}
        AND cs.run_id = {run_id}
        ORDER BY cs.cluster_label;

        """,
        conn,
    )

    if df.empty:
        conn.close()
        raise ValueError("❌ Nenhum setor encontrado")

    # =========================================================
    # 📍 PDVs (fonte real para métricas geométricas)
    # =========================================================
    df_pdvs = pd.read_sql_query(
        f"""
        SELECT
            cs.id AS cluster_id,
            cs.centro_lat,
            cs.centro_lon,
            p.id AS pdv_id,
            p.pdv_lat,
            p.pdv_lon,
            p.pdv_vendas
        FROM cluster_setor cs
        JOIN cluster_setor_pdv csp
          ON csp.cluster_id = cs.id
         AND csp.tenant_id = cs.tenant_id
        JOIN pdvs p
          ON p.id = csp.pdv_id
         AND p.tenant_id = cs.tenant_id
        WHERE cs.run_id = {run_id}
          AND cs.tenant_id = {tenant_id}
          AND p.pdv_lat IS NOT NULL
          AND p.pdv_lon IS NOT NULL;
        """,
        conn,
    )

    conn.close()
    
    # =========================================================
    # 💰 Vendas por cluster (soma PDVs)
    # =========================================================
    if "pdv_vendas" in df_pdvs.columns:
        df_vendas = (
            df_pdvs
            .groupby("cluster_id", as_index=False)["pdv_vendas"]
            .sum()
            .rename(columns={"pdv_vendas": "Vendas PDVs"})
        )

        df = df.merge(df_vendas, on="cluster_id", how="left")
        df["Vendas PDVs"] = df["Vendas PDVs"].fillna(0)
    else:
        df["Vendas PDVs"] = 0

    # =======================================================
    # 🧮 Recalcular métricas reais (override seguro)
    # =========================================================
    if not df_pdvs.empty:
        df_calc = calcular_metricas_por_cluster(df_pdvs)

        df = df.merge(
            df_calc,
            on="cluster_id",
            how="left",
            suffixes=("", "_calc"),
        )

        for col in [
            "dist_media_km",
            "dist_max_km",
            "tempo_medio_min",
            "tempo_max_min",
        ]:
            col_calc = f"{col}_calc"
            if col_calc in df.columns:
                df[col] = df[col_calc].fillna(df[col])

        df.drop(columns=[c for c in df.columns if c.endswith("_calc")], inplace=True)

    # =========================================================
    # 🧭 Reverse geocode — REGRA CORRETA
    # =========================================================
    if "origem_geocode" in df.columns:
        precisa_reverse = df["origem_geocode"].isin(
            ["gerado_algoritmo", "kmeans", "auto"]
        ).any()
    else:
        # fallback defensivo (nunca deveria acontecer)
        precisa_reverse = True

    if precisa_reverse:
        logger.info("🧭 Reverse geocode aplicado (modo normal)")
        mask_reverse = df["origem_geocode"].isin(["gerado_algoritmo", "kmeans", "auto"])
        df_geo = enrich_centros_reverse(df[mask_reverse])

        df = df.merge(
            df_geo,
            on=["centro_lat", "centro_lon"],
            how="left",
        )

        # Garantia defensiva de colunas pós-reverse
        for col in ["Endereco centro", "Cidade centro", "UF centro"]:
            if col not in df.columns:
                df[col] = ""


    else:
        logger.info("ℹ️ Reverse ignorado (ativo_balanceado)")
        df["Endereco centro"] = df["endereco_centro"]
        df["Cidade centro"] = ""
        df["UF centro"] = ""


    # =========================================================
    # 🏷️ Renomeio executivo
    # =========================================================
    df = df.rename(
        columns={
            "cluster_label": "Cluster",
            "enderecos": "Endereços",
            "Vendas PDVs": "Vendas PDVs",
            "cnpj_centro": "CNPJ Centro",
            "bandeira": "Bandeira",
            "dist_media_km": "Distância média (km)",
            "dist_max_km": "Distância máxima (km)",
            "tempo_medio_min": "Tempo médio (min)",
            "tempo_max_min": "Tempo máximo (min)",
            "Endereco centro": "Endereço do Centro",
            "Cidade centro": "Cidade",
            "UF centro": "UF",
            "centro_lat": "Latitude centro",
            "centro_lon": "Longitude centro",
        }
    )

    df = df[
        [
            "Cluster",
            "Endereços",
            "Vendas PDVs",
            "CNPJ Centro",
            "Bandeira",
            "Distância média (km)",
            "Distância máxima (km)",
            "Tempo médio (min)",
            "Tempo máximo (min)",
            "Endereço do Centro",
            "Cidade",
            "UF",
            "Latitude centro",
            "Longitude centro",
        ]
    ]

    # =========================================================
    # 📤 Excel (SaaS-ready)
    # =========================================================
    output_dir = f"output/reports/{tenant_id}"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(
        output_dir,
        f"cluster_resumo_{clusterization_id}.xlsx",
    )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Resumo por Cluster", index=False)

    logger.success(f"✅ Excel gerado: {output_path}")


# =========================================================
# 🚀 CLI
# =========================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant_id", type=int, required=True)
    parser.add_argument("--clusterization_id", type=str, required=True)
    args = parser.parse_args()

    exportar_cluster_resumo(args.tenant_id, args.clusterization_id)
