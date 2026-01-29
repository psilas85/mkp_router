#mkp_clusterization/application/cluster_use_case.py

from typing import Optional, Dict, Any, List
from loguru import logger
import numpy as np
from sklearn.neighbors import NearestNeighbors
import math
from sklearn.cluster import KMeans
from src.mkp_clusterization.domain.haversine_utils import haversine


from src.mkp_clusterization.infrastructure.persistence.database_reader import carregar_pdvs
from src.mkp_clusterization.infrastructure.persistence.database_writer import (
    criar_run,
    finalizar_run,
    salvar_setores,
    salvar_mapeamento_pdvs,
    salvar_outliers,
    salvar_centros,
)
from src.mkp_clusterization.infrastructure.logging.run_logger import snapshot_params

from src.mkp_clusterization.domain.entities import PDV, Setor
from src.mkp_clusterization.domain.k_estimator import estimar_k_inicial
from src.mkp_clusterization.domain.operational_cluster_refiner import OperationalClusterRefiner

# 🔵 KMeans balanceado (teto por cluster)
from src.mkp_clusterization.domain.sector_generator import kmeans_balanceado

# 🟢 Sweep com capacidade
from src.mkp_clusterization.domain.capacitated_sweep import capacitated_sweep
from src.mkp_clusterization.domain.dense_subset import dense_subset



# ============================================================
# 🧠 Detecção simplificada de outliers geográficos
# ============================================================
def detectar_outliers_geograficos(pdvs: List[PDV], z_thresh: float = 1.5):
    if len(pdvs) < 5:
        return [(p, False) for p in pdvs]

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    coords_rad = np.radians(coords)

    nn = NearestNeighbors(n_neighbors=min(6, len(coords)), metric="haversine")
    nn.fit(coords_rad)

    dist, _ = nn.kneighbors(coords_rad)
    dist_min = dist[:, 1] * 6371.0  # km

    mean = np.mean(dist_min)
    std = np.std(dist_min)

    limiar = mean + z_thresh * std
    flags = dist_min > limiar

    logger.info(f"🧹 Outliers detectados={np.sum(flags)}/{len(pdvs)} | limiar={limiar:.2f} km")
    return [(pdvs[i], bool(flags[i])) for i in range(len(pdvs))]


# ============================================================
# 🚀 Execução principal  (VERSÃO COMPLETA CORRIGIDA)
# ============================================================
from time import time  # ✅ tempo real da execução

def executar_clusterizacao(
    tenant_id: int,
    uf: Optional[str],
    cidade: Optional[str],
    algo: str,
    dias_uteis: int,
    freq: int,
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float,
    alpha_path: float,
    max_pdv_cluster: int,
    descricao: str,
    input_id: str,
    clusterization_id: str,
    excluir_outliers: bool,
    z_thresh: float,
    max_iter: int,
) -> Dict[str, Any]:

    inicio_execucao = time()  # ✅ start cronômetro

    logger.info(f"🏁 Iniciando clusterização | tenant={tenant_id} | algo={algo}")

    # ============================================================
    # ❗ Regra de negócio (defensiva)
    # ============================================================
    if algo == "capacitated_sweep":
        if not cidade or not str(cidade).strip():
            raise ValueError(
                "Cidade obrigatória para execução do algoritmo capacitated_sweep."
            )


    # ============================================================
    # 1) Carregar PDVs
    # ============================================================
    pdvs = carregar_pdvs(tenant_id, input_id, uf, cidade)
    if not pdvs:
        raise ValueError("Nenhum PDV encontrado.")

    logger.info(f"📦 {len(pdvs)} PDVs carregados.")

    # ============================================================
    # 1.5) Remover duplicados (evita falso outlier)
    # ============================================================
    pdvs = list({(p.lat, p.lon, p.cnpj): p for p in pdvs}.values())

    # ============================================================
    # 2) Outliers
    # ============================================================
    pdv_flags = detectar_outliers_geograficos(pdvs, z_thresh)
    total_outliers = sum(1 for _, f in pdv_flags if f)

    salvar_outliers(
        tenant_id,
        clusterization_id,
        [
            {
                "pdv_id": getattr(p, "id", None),
                "lat": p.lat,
                "lon": p.lon,
                "is_outlier": bool(flag),
            }
            for p, flag in pdv_flags
        ],
    )

    if excluir_outliers:
        pdvs = [p for p, f in pdv_flags if not f]
        logger.info(f"🧹 {total_outliers} outliers removidos.")

    # ============================================================
    # 3) Registrar execução
    # ============================================================
    params = snapshot_params(
        uf=uf,
        cidade=cidade,
        algo=algo,
        k_forcado=None,
        dias_uteis=dias_uteis,
        freq=freq,
        workday_min=workday_min,
        route_km_max=route_km_max,
        service_min=service_min,
        v_kmh=v_kmh,
        alpha_path=alpha_path,
        n_pdvs=len(pdvs),
        max_pdv_cluster=max_pdv_cluster,
        descricao=descricao,
        input_id=input_id,
        clusterization_id=clusterization_id,
    )

    run_id = criar_run(
        tenant_id=tenant_id,
        uf=uf,
        cidade=cidade,
        algo=algo,
        params=params,
        descricao=descricao,
        input_id=input_id,
        clusterization_id=clusterization_id,
    )

    try:
        
        # ============================================================
        # ⚪ KMEANS PURE — modelo CEP adaptado p/ PDV (tempo-based)
        # ============================================================
        # Aqui workday_min JÁ representa tempo_max_min (em minutos)
        tempo_max_min = workday_min

        if algo == "kmeans_pure":
            logger.info("⚪ Executando KMEANS_PURE (CEP-like, sem refinamento operacional).")
            logger.info(f"⏱️ Tempo máximo centro → PDV = {tempo_max_min} min")

            coords = np.array([[p.lat, p.lon] for p in pdvs], dtype=float)

            # ----------------------------
            # Peso neutro (1.0) por PDV
            # ----------------------------
            pesos = np.ones(len(pdvs), dtype=float)
            pesos_norm = pesos / pesos.max() if pesos.max() > 0 else 1.0

            # ----------------------------
            # 🧮 Estimativa inicial de K POR CAPACIDADE
            # ----------------------------
            n_pdvs = len(pdvs)

            if not max_pdv_cluster or max_pdv_cluster <= 0:
                raise ValueError(
                    "max_pdv_cluster deve ser informado e > 0 para uso do kmeans_pure"
                )

            k_atual = math.ceil(n_pdvs / max_pdv_cluster)

            logger.info(
                f"🧮 K inicial por capacidade | "
                f"n_pdvs={n_pdvs} | "
                f"max_pdv_cluster={max_pdv_cluster} | "
                f"k_inicial={k_atual}"
            )

            # ----------------------------
            # Loop adaptativo: TEMPO + CAPACIDADE
            # ----------------------------
            labels = None
            for tentativa in range(max_iter):
                logger.info(f"🔄 Tentativa {tentativa+1}/{max_iter}: KMeans com k={k_atual}")

                kmeans = KMeans(n_clusters=k_atual, random_state=42, n_init=10)
                labels = kmeans.fit_predict(coords)

                tempo_max_global = 0.0
                pdvs_max_cluster = 0

                for cid in sorted(set(labels)):
                    idx = np.where(labels == cid)[0]
                    pdvs_cluster = [pdvs[i] for i in idx]

                    if not pdvs_cluster:
                        continue

                    # Centro ponderado (peso neutro)
                    centro_lat = float(np.average(
                        [p.lat for p in pdvs_cluster],
                        weights=pesos_norm[idx]
                    ))
                    centro_lon = float(np.average(
                        [p.lon for p in pdvs_cluster],
                        weights=pesos_norm[idx]
                    ))

                    tempo_max_cluster = 0.0
                    for p in pdvs_cluster:
                        dist_km = haversine((p.lat, p.lon), (centro_lat, centro_lon))
                        tempo_min = (dist_km / v_kmh) * 60 if v_kmh > 0 else 999999
                        tempo_max_cluster = max(tempo_max_cluster, tempo_min)

                    tempo_max_global = max(tempo_max_global, tempo_max_cluster)
                    pdvs_max_cluster = max(pdvs_max_cluster, len(pdvs_cluster))

                logger.info(
                    f"⏱️ tempo_max_global={tempo_max_global:.2f} min | "
                    f"pdvs_max_cluster={pdvs_max_cluster} | k={k_atual}"
                )

                tempo_ok = tempo_max_global <= tempo_max_min
                capacidade_ok = pdvs_max_cluster <= max_pdv_cluster

                if tempo_ok and capacidade_ok:
                    logger.info("✅ Critérios atendidos (tempo máximo e capacidade).")
                    break

                if tentativa == max_iter - 1:
                    logger.warning(
                        "⚠️ Limite de iterações atingido — encerrando com o melhor K encontrado."
                    )
                    break

                k_atual += 1

            # ----------------------------
            # Geração dos setores finais
            # ----------------------------
            setores_finais = []
            for cid in sorted(set(labels)):
                idx = np.where(labels == cid)[0]
                pdvs_cluster = [pdvs[i] for i in idx]
                if not pdvs_cluster:
                    continue

                centro_lat = float(np.mean([p.lat for p in pdvs_cluster]))
                centro_lon = float(np.mean([p.lon for p in pdvs_cluster]))

                setores_finais.append(
                    {
                        "cluster_label": int(cid),
                        "centro_lat": float(centro_lat),
                        "centro_lon": float(centro_lon),
                        "n_pdvs": len(pdvs_cluster),
                        "raio_med_km": 0.0,
                        "raio_p95_km": 0.0,
                        "metrics": {},
                        "subclusters": [],
                    }
                )


                for p in pdvs_cluster:
                    p.cluster_label = int(cid)

                
        # ============================================================
        # 🔵 KMEANS — refinamento operacional
        # ============================================================
        elif algo == "kmeans":
            logger.info("🔵 Executando KMEANS + refinamento.")

            # (opcional) se você não usa o retorno, pode remover esta linha
            _ = kmeans_balanceado(
                pdvs=pdvs,
                max_pdv_cluster=max_pdv_cluster,
                v_kmh=v_kmh,
                max_dist_km=route_km_max,
                max_time_min=workday_min,
                tempo_servico_min=service_min,
            )

            refiner = OperationalClusterRefiner(
                v_kmh=v_kmh,
                max_time_min=workday_min,
                max_dist_km=route_km_max,
                tempo_servico_min=service_min,
                max_iter=max_iter,
                tenant_id=tenant_id,
            )

            setores_finais = refiner.refinar_com_subclusters_iterativo(
                pdvs=pdvs,
                dias_uteis=dias_uteis,
                freq=freq,
                max_pdv_cluster=max_pdv_cluster,
            )

        # ============================================================
        # 🟢 CAPACITATED SWEEP
        # ============================================================
        elif algo == "capacitated_sweep":
            logger.info("🟢 Executando CAPACITATED SWEEP.")
            labels, centers = capacitated_sweep(pdvs, max_capacity=max_pdv_cluster)

            setores_finais = []
            for cid, c in enumerate(centers):
                cluster_points = [p for p, lbl in zip(pdvs, labels) if lbl == cid]
                if not cluster_points:
                    continue

                setores_finais.append(
                    Setor(
                        cluster_label=cid,
                        centro_lat=float(c[0]),
                        centro_lon=float(c[1]),
                        n_pdvs=len(cluster_points),
                        raio_med_km=0,
                        raio_p95_km=0,
                    )
                )
                for p in cluster_points:
                    p.cluster_label = cid

        # ============================================================
        # 🟣 DENSE SUBSET — cluster único
        # ============================================================
        elif algo == "dense_subset":
            logger.info(f"🟣 Executando DENSE SUBSET | capacidade={max_pdv_cluster}")

            selecionados = dense_subset(pdvs, capacidade=max_pdv_cluster)

            lat_med = float(np.mean([p.lat for p in selecionados]))
            lon_med = float(np.mean([p.lon for p in selecionados]))

            setores_finais = [
                Setor(
                    cluster_label=0,
                    centro_lat=lat_med,
                    centro_lon=lon_med,
                    n_pdvs=len(selecionados),
                    raio_med_km=0,
                    raio_p95_km=0,
                )
            ]

            for p in selecionados:
                p.cluster_label = 0

            pdvs = selecionados

        else:
            raise ValueError(f"Algoritmo inválido: {algo}")

        # ============================================================
        # 💾 Persistência
        # ============================================================

        # 🔑 Normalização única e consistente
        # 🔑 Normalização única e consistente
        labels_orig = sorted({s["cluster_label"] for s in setores_finais})

        mapa = {old: new for new, old in enumerate(labels_orig)}

        for s in setores_finais:
            s["cluster_label"] = mapa[s["cluster_label"]]


        for p in pdvs:
            if p.cluster_label in mapa:
                p.cluster_label = mapa[p.cluster_label]

        # ============================================================
        # 🏭 Persistir CENTROS GERADOS (modo normal)
        # ============================================================
        def _centro_endereco_placeholder() -> str:
            partes = []
            if cidade and str(cidade).strip():
                partes.append(str(cidade).strip())
            if uf and str(uf).strip():
                partes.append(str(uf).strip())
            sufixo = " - ".join(partes) if partes else "BR"
            return f"CENTRO GERADO - {sufixo}"

        centros_gerados = []
        for s in setores_finais:
            centros_gerados.append(
                {
                    "cluster_label": int(s["cluster_label"]),
                    "lat": float(s["centro_lat"]),
                    "lon": float(s["centro_lon"]),
                    "endereco": _centro_endereco_placeholder(),
                    "origem": "gerado_algoritmo",
                    "cnpj": None,
                    "bandeira": None,
                }
            )

        centro_id_map = salvar_centros(
            tenant_id=tenant_id,
            input_id=input_id,
            clusterization_id=clusterization_id,
            run_id=run_id,
            centros=centros_gerados,
        )

        # ============================================================
        # 🧩 Recriar Setores com centro_id (contrato da entidade)
        # ============================================================
        setores_ok: List[Setor] = []

        for s in setores_finais:
            cid = int(s["cluster_label"])
            centro_id = centro_id_map.get(cid)

            if centro_id is None:
                raise ValueError(f"centro_id não encontrado para cluster_label={cid}")

            setores_ok.append(
                Setor(
                    cluster_label=cid,
                    centro_id=centro_id,
                    centro_lat=float(s["centro_lat"]),
                    centro_lon=float(s["centro_lon"]),
                    n_pdvs=int(s["n_pdvs"]),
                    raio_med_km=float(s["raio_med_km"]),
                    raio_p95_km=float(s["raio_p95_km"]),
                    metrics=dict(s.get("metrics", {})),
                    subclusters=list(s.get("subclusters", [])),
                )
            )

        setores_finais = setores_ok



        
        # ============================================================
        # 💾 Persistência (setores + mapeamento)
        # ============================================================
        mapping = salvar_setores(tenant_id, run_id, setores_finais)


        for p in pdvs:
            p.cluster_id = mapping[p.cluster_label]

        salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)

        finalizar_run(run_id, status="done", k_final=len(setores_finais))

        # ============================================================
        # 📘 Atualiza histórico do job (tempo REAL)
        # ============================================================
        from src.mkp_clusterization.infrastructure.persistence.database_writer import (
            atualizar_historico_cluster_job,
        )

        duracao_segundos = time() - inicio_execucao  # ✅ tempo real

        atualizar_historico_cluster_job(
            tenant_id=tenant_id,
            job_id=clusterization_id,  # ✅ seu job_id é o UUID do clusterization_id
            k_final=len(setores_finais),
            n_pdvs=len(pdvs),
            duracao_segundos=float(duracao_segundos),
            status="done",
        )

        return {
            "tenant_id": tenant_id,
            "clusterization_id": clusterization_id,
            "run_id": run_id,
            "algo": algo,
            "k_final": len(setores_finais),
            "n_pdvs": len(pdvs),
            "outliers": total_outliers,
            "setores": [
                {
                    "cluster_label": s.cluster_label,
                    "centro_lat": s.centro_lat,
                    "centro_lon": s.centro_lon,
                    "n_pdvs": s.n_pdvs,
                }
                for s in setores_finais
            ],
        }

    except Exception as e:
        logger.error(f"❌ Erro durante clusterização: {e}")
        finalizar_run(run_id, status="error", k_final=0, error=str(e))
        raise
