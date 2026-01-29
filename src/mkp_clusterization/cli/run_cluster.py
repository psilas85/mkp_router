#mkp_router/src/mkp_clusterization/cli/run_cluster.py

import argparse
import uuid
from loguru import logger
from src.mkp_clusterization.application.cluster_use_case import executar_clusterizacao


UF_VALIDAS = {
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO",
}


def validar_uf(uf: str):
    uf = uf.strip().upper()
    if uf not in UF_VALIDAS:
        raise ValueError(f"UF inválida: {uf}")
    return uf


def validar_input_id(input_id: str):
    try:
        return str(uuid.UUID(input_id))
    except Exception:
        raise ValueError(f"input_id inválido: '{input_id}' — deve ser um UUID válido.")


def main():

    parser = argparse.ArgumentParser(
        description="Clusterização de PDVs (MKP Router | multi-tenant)"
    )

    # ============================================================
    # Obrigatórios
    # ============================================================
    parser.add_argument("--tenant_id", type=int, required=True)
    parser.add_argument("--uf", required=True)
    parser.add_argument("--descricao", required=True)
    parser.add_argument("--input_id", required=True)

    # ============================================================
    # Opcionais
    # ============================================================
    parser.add_argument("--cidade")

    parser.add_argument(
        "--algo",
        type=str,
        choices=[
            "kmeans",
            "kmeans_pure",
            "capacitated_sweep",
            "dense_subset",
            "ativo_balanceado",
        ],
        default="kmeans",
        help="Algoritmo de clusterização"
    )

    # ============================================================
    # Parâmetros OPERACIONAIS (kmeans clássico)
    # ============================================================
    parser.add_argument("--dias_uteis", type=int, default=20)
    parser.add_argument("--freq", type=int, default=1)
    parser.add_argument("--routekm", type=float, default=200.0)
    parser.add_argument("--service", type=int, default=30)
    parser.add_argument("--vel", type=float, default=35.0)

    # ============================================================
    # 🔥 tempo máximo por cluster (CEP-like)
    # ============================================================
    parser.add_argument(
        "--tempo_max_min",
        type=int,
        default=15,
        help="Tempo máximo (min) do centro do cluster até o PDV"
    )

    # ============================================================
    # 🔥 ATIVO BALANCEADO
    # ============================================================
    parser.add_argument("--centros_csv", help="CSV com endereços dos centros")
    parser.add_argument("--min_pdv", type=int, help="Mínimo de PDVs por centro")

    # ============================================================
    # Gerais
    # ============================================================
    parser.add_argument("--max_pdv_cluster", type=int, default=200)
    parser.add_argument("--max_iter", type=int, default=10)
    parser.add_argument("--excluir_outliers", action="store_true")
    parser.add_argument("--clusterization_id")
    parser.add_argument("--z_thresh", type=float, default=3.0)

    args = parser.parse_args()

    # ============================================================
    # Validações
    # ============================================================
    uf = validar_uf(args.uf)
    input_id = validar_input_id(args.input_id)

    cidade = (
        args.cidade.strip()
        if args.cidade and args.cidade.strip().lower() not in ("none", "")
        else None
    )

    clusterization_id = args.clusterization_id or str(uuid.uuid4())

    # ============================================================
    # Logs
    # ============================================================
    logger.info("==============================================")
    logger.info("🚀 Iniciando clusterização via CLI")
    logger.info("==============================================")
    logger.info(f"🔑 tenant_id         = {args.tenant_id}")
    logger.info(f"📦 input_id          = {input_id}")
    logger.info(f"🗺️ UF                = {uf}")
    logger.info(f"🏙️ cidade            = {cidade or 'ALL'}")
    logger.info(f"⚙️ algoritmo         = {args.algo}")
    logger.info(f"📝 descrição         = {args.descricao}")
    logger.info(f"🆔 clusterization_id = {clusterization_id}")
    logger.info(f"⏱️ tempo_max_min     = {args.tempo_max_min} min")

    logger.info("----- Parâmetros -----")
    logger.info(f"🔢 max_pdv_cluster    = {args.max_pdv_cluster}")
    logger.info(f"🔧 max_iter           = {args.max_iter}")
    logger.info(f"🧹 excluir_outliers   = {args.excluir_outliers}")
    logger.info(f"📏 z_thresh           = {args.z_thresh}")
    logger.info(f"🚚 velocidade (km/h)  = {args.vel}")

    # ============================================================
    # Execução
    # ============================================================
    if args.algo == "ativo_balanceado":
        from src.mkp_clusterization.application.cluster_ativo_balanceado_use_case import (
            ClusterAtivoBalanceadoUseCase,
        )

        if not args.centros_csv:
            raise ValueError("--centros_csv é obrigatório para ativo_balanceado")

        if args.min_pdv is None:
            raise ValueError("--min_pdv é obrigatório para ativo_balanceado")

        use_case = ClusterAtivoBalanceadoUseCase(
            tenant_id=args.tenant_id,
            uf=uf,
            cidade=cidade,
            input_id=input_id,
            descricao=args.descricao,
            centros_csv=args.centros_csv,
            min_pdv=args.min_pdv,
            max_pdv=args.max_pdv_cluster,
            tempo_max_min=args.tempo_max_min,
            v_kmh=args.vel,
            max_iter=args.max_iter,
            clusterization_id=clusterization_id,   # 🔥 CONSISTENTE
        )

        result = use_case.execute()

    else:
        result = executar_clusterizacao(
            tenant_id=args.tenant_id,
            uf=uf,
            cidade=cidade,
            algo=args.algo,
            dias_uteis=args.dias_uteis,
            freq=args.freq,
            workday_min=args.tempo_max_min,
            route_km_max=args.routekm,
            service_min=args.service,
            v_kmh=args.vel,
            alpha_path=1.0,
            max_pdv_cluster=args.max_pdv_cluster,
            descricao=args.descricao,
            input_id=input_id,
            clusterization_id=clusterization_id,
            excluir_outliers=args.excluir_outliers,
            z_thresh=args.z_thresh,
            max_iter=args.max_iter,
        )

    print("\n=== RESULTADO FINAL ===")
    for campo in ("clusterization_id", "run_id", "k_final", "n_pdvs"):
        print(f"{campo}: {result.get(campo, 'N/A')}")


if __name__ == "__main__":
    main()
