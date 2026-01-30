#mkp_router/src/mkp_clusterization/application/cluster_ativo_balanceado_use_case.py

import uuid
import time
import os
import re
import pandas as pd
from loguru import logger
from typing import Optional, Dict, Any, List

from src.mkp_clusterization.domain.entities import PDV, Setor
from src.mkp_clusterization.domain.haversine_utils import haversine
from src.mkp_clusterization.domain.centers_geolocation_service import CentersGeolocationService
from src.mkp_clusterization.domain.pdv_cluster_balanceador import balancear_clusters_pdv
from src.database.db_connection import get_connection
from src.mkp_clusterization.domain.metrics_calculator import calcular_metricas_cluster
from src.mkp_clusterization.infrastructure.persistence.database_writer import salvar_centros
from mkp_preprocessing.domain.utils_geo import coordenada_generica


from src.mkp_clusterization.infrastructure.persistence.database_reader import (
    carregar_pdvs,
    DatabaseReader,
)
from src.mkp_clusterization.infrastructure.persistence.database_writer import (
    criar_run,
    finalizar_run,
    salvar_setores,
    salvar_mapeamento_pdvs,
    atualizar_historico_cluster_job,
    DatabaseWriter,
)
from src.mkp_clusterization.infrastructure.logging.run_logger import snapshot_params


class ClusterAtivoBalanceadoUseCase:
    """
    Clusterização Ativa Balanceada (PDV)

    Estratégia (CEP-inspired):
    - Centros fornecidos pelo usuário (CSV)
    - Centros FIXOS (não se movem)
    - Atribuição inicial PDV → centro mais próximo
    - Balanceamento iterativo:
        • resolve excesso (> max_pdv)
        • resolve déficit (< min_pdv) descartando centros
        • move sempre PDVs da borda
    """

    def __init__(
        self,
        tenant_id: int,
        uf: Optional[str],
        cidade: Optional[str],
        input_id: str,
        descricao: str,
        centros_csv: str,
        min_pdv: int,
        max_pdv: int,
        tempo_max_min: int,
        v_kmh: float = 35.0,
        max_iter: int = 10,
        clusterization_id: str | None = None,
    ):
        self.tenant_id = tenant_id
        self.uf = uf
        self.cidade = cidade
        self.input_id = input_id
        self.descricao = descricao
        self.centros_csv = centros_csv
        self.min_pdv = min_pdv
        self.max_pdv = max_pdv
        self.tempo_max_min = tempo_max_min
        self.v_kmh = v_kmh
        self.max_iter = max_iter

        # 🔥 FONTE ÚNICA DE VERDADE
        self.clusterization_id = clusterization_id or str(uuid.uuid4())

        self.conn = get_connection()
        self.reader = DatabaseReader(self.conn)
        self.writer = DatabaseWriter(self.conn)

        enable_google = os.getenv("ENABLE_GOOGLE_GEOCODING", "false").lower() == "true"

        self.centros_geo = CentersGeolocationService(
            reader=self.reader,
            writer=self.writer,
            google_key=os.getenv("GMAPS_API_KEY") if enable_google else None,
        )



    # ============================================================
    # ▶️ Execução principal
    # ============================================================
    def execute(self) -> Dict[str, Any]:
        inicio_execucao = time.time()

        logger.info("==============================================")
        logger.info("🚀 Iniciando CLUSTERIZAÇÃO ATIVA BALANCEADA")
        logger.info("==============================================")

        # ============================================================
        # 1) Carregar PDVs
        # ============================================================
        pdvs: List[PDV] = carregar_pdvs(
            self.tenant_id,
            self.input_id,
            self.uf,
            self.cidade,
        )

        if not pdvs:
            raise ValueError("Nenhum PDV encontrado.")

        logger.info(f"📦 PDVs carregados: {len(pdvs)}")

        # ============================================================
        # 2) Carregar + geocodificar centros
        # ============================================================
        centros = self._carregar_centros()
        if not centros:
            raise ValueError("Nenhum centro válido após geocodificação.")

        logger.info(f"🏭 Centros válidos: {len(centros)}")

        # ============================================================
        # 3) Registrar RUN
        # ============================================================
        params = snapshot_params(
            uf=self.uf,
            cidade=self.cidade,
            algo="ativo_balanceado",
            k_forcado=len(centros),
            dias_uteis=None,
            freq=None,
            workday_min=self.tempo_max_min,
            route_km_max=None,
            service_min=None,
            v_kmh=self.v_kmh,
            alpha_path=None,
            n_pdvs=len(pdvs),
            max_pdv_cluster=self.max_pdv,
            descricao=self.descricao,
            input_id=self.input_id,
            clusterization_id=self.clusterization_id,
        )

        run_id = criar_run(
            tenant_id=self.tenant_id,
            uf=self.uf,
            cidade=self.cidade,
            algo="ativo_balanceado",
            params=params,
            descricao=self.descricao,
            input_id=self.input_id,
            clusterization_id=self.clusterization_id,
        )

        # ============================================================
        # 3.1) Persistir CENTROS como entidade
        # ============================================================
        centro_id_map = salvar_centros(
            tenant_id=self.tenant_id,
            input_id=self.input_id,
            clusterization_id=self.clusterization_id,
            run_id=run_id,
            centros=centros,
        )

        # injeta centro_id nos centros em memória
        for c in centros:
            c["centro_id"] = centro_id_map.get(int(c["cluster_label"]))


        try:
            # ============================================================
            # 4) Atribuição inicial PDV → centro mais próximo
            # ============================================================
            self._atribuir_pdvs_a_centros(pdvs, centros)

            # ============================================================
            # 5) Balanceamento CEP-inspired
            # ============================================================
            pdvs, centros = balancear_clusters_pdv(
                pdvs=pdvs,
                centros=centros,
                min_pdv=self.min_pdv,
                max_pdv=self.max_pdv,
                tempo_max_min=self.tempo_max_min,
                v_kmh=self.v_kmh,
                max_iter=self.max_iter,
                max_merge_km=10.0,
            )

            # ============================================================
            # 5.1) Normalizar labels após descarte de centros
            # ============================================================
            labels_orig = sorted({int(c["cluster_label"]) for c in centros})
            mapa = {old: new for new, old in enumerate(labels_orig)}

            for c in centros:
                c["cluster_label"] = mapa[int(c["cluster_label"])]

            for p in pdvs:
                p.cluster_label = mapa[int(p.cluster_label)]

            # ============================================================
            # 6) Gerar setores finais (centros FIXOS)
            # ============================================================
            setores = self._gerar_setores(pdvs, centros)

            # ============================================================
            # 7) Persistência (INALTERADA)
            # ============================================================
            mapping = salvar_setores(self.tenant_id, run_id, setores)

            for p in pdvs:
                p.cluster_id = mapping[p.cluster_label]

            salvar_mapeamento_pdvs(self.tenant_id, run_id, pdvs)
            finalizar_run(run_id, status="done", k_final=len(setores))

            duracao = time.time() - inicio_execucao

            atualizar_historico_cluster_job(
                tenant_id=self.tenant_id,
                job_id=self.clusterization_id,
                k_final=len(setores),
                n_pdvs=len(pdvs),
                duracao_segundos=float(duracao),
                status="done",
            )

            logger.success(
                f"🏁 Clusterização ATIVA BALANCEADA concluída | centros={len(setores)}"
            )

            return {
                "status": "done",
                "tenant_id": self.tenant_id,
                "clusterization_id": self.clusterization_id,
                "run_id": run_id,
                "k_final": len(setores),
                "n_pdvs": len(pdvs),
                "duracao_segundos": round(duracao, 2),
            }

        except Exception as e:
            logger.error(f"❌ Erro na clusterização ativa balanceada: {e}")
            finalizar_run(run_id, status="error", k_final=0, error=str(e))
            raise

    def _carregar_centros(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.centros_csv):
            raise FileNotFoundError(self.centros_csv)

        # ============================================================
        # 📊 Estatísticas de diagnóstico
        # ============================================================
        stats = {
            "lidos": 0,
            "endereco_invalido": 0,
            "geocode_falha": 0,
            "coord_generica": 0,
            "duplicado_coord": 0,
            "validos": 0,
        }

        # ============================================================
        # 1) Leitura do arquivo (XLSX ou CSV)
        # ============================================================
        ext = os.path.splitext(self.centros_csv)[1].lower()

        if ext == ".xlsx":
            df = pd.read_excel(self.centros_csv)
        elif ext == ".csv":
            df = pd.read_csv(self.centros_csv, sep=None, engine="python")
        else:
            raise ValueError("Formato inválido. Use .xlsx ou .csv")

        stats["lidos"] = len(df)

        # ============================================================
        # 2) Normalização de colunas
        # ============================================================
        df.columns = [c.strip().lower() for c in df.columns]

        df = df.rename(columns={
            "bandeira cliente": "bandeira",
            "bandeira_cliente": "bandeira",
            "cnpj": "cnpj",
            "razao": "razao_social",
        })

        # ============================================================
        # 3) Validação mínima de schema (fail fast)
        # ============================================================
        obrigatorias = ["logradouro", "bairro", "cidade", "uf"]
        faltantes = [c for c in obrigatorias if c not in df.columns]
        if faltantes:
            raise ValueError(
                f"Colunas obrigatórias ausentes no arquivo de centros: {faltantes}"
            )

        for c in ["numero", "cep", "cnpj", "bandeira"]:
            if c not in df.columns:
                df[c] = ""

        # ============================================================
        # 4) Helpers
        # ============================================================
        def limpar(v):
            if pd.isna(v):
                return ""
            return re.sub(r"\s+", " ", str(v).strip())

        def separar_numero(v):
            if not v:
                return "", None
            v = str(v).strip()
            m = re.search(r"(.*?)[, ]+(\d+[A-Za-z\-\/]*)$", v)
            if m:
                return m.group(1).strip(), m.group(2).strip()
            return v, None

        df["uf"] = df["uf"].astype(str).str.strip().str.upper()
        df["cidade"] = df["cidade"].astype(str).str.strip()
        df["bairro"] = df["bairro"].astype(str).str.strip()

        # ============================================================
        # 5) Montagem do endereço completo
        # ============================================================
        def montar_endereco(r):
            logradouro = limpar(r["logradouro"])
            numero = limpar(r["numero"])

            if not numero:
                logradouro, numero_extraido = separar_numero(logradouro)
                if numero_extraido:
                    numero = numero_extraido

            cidade = limpar(r["cidade"]) or (self.cidade or "")
            uf = limpar(r["uf"]) or (self.uf or "")

            if not logradouro or not cidade or not uf:
                return ""

            partes = [
                logradouro,
                numero,
                limpar(r["bairro"]),
                cidade,
                uf,
                limpar(r["cep"]),
            ]

            return " - ".join([p for p in partes if p])

        df["endereco_full"] = df.apply(montar_endereco, axis=1)
        df["endereco_full"] = df["endereco_full"].str.strip()

        # descarta endereços inválidos
        invalidos = df["endereco_full"].str.len() <= 10
        stats["endereco_invalido"] += int(invalidos.sum())
        df = df[~invalidos].copy()

        df = df.drop_duplicates(subset=["endereco_full"])

        # ============================================================
        # 6) Geocodificação + montagem dos centros
        # ============================================================
        centros: List[Dict[str, Any]] = []

        for _, r in df.iterrows():
            endereco = r["endereco_full"]

            lat, lon, origem = self.centros_geo.buscar(endereco)

            if lat is None or lon is None:
                stats["geocode_falha"] += 1
                logger.debug(
                    f"❌ Centro descartado | motivo=geocode_falha | endereco='{endereco}'"
                )
                continue

            if coordenada_generica(lat, lon):
                stats["coord_generica"] += 1
                logger.debug(
                    f"❌ Centro descartado | motivo=coord_generica | endereco='{endereco}'"
                )
                continue

            cnpj = limpar(r.get("cnpj"))
            cnpj = re.sub(r"\D", "", cnpj)
            bandeira = limpar(r.get("bandeira"))

            centros.append(
                {
                    "cluster_label": len(centros),
                    "lat": float(lat),
                    "lon": float(lon),
                    "endereco": endereco,
                    "origem": origem,
                    "cnpj": cnpj,
                    "bandeira": bandeira,
                }
            )

        # ============================================================
        # 7) Deduplicação por coordenada
        # ============================================================
        uniq: Dict[tuple, Dict[str, Any]] = {}
        for c in centros:
            k = (round(c["lat"], 6), round(c["lon"], 6))
            if k not in uniq:
                uniq[k] = c
            else:
                stats["duplicado_coord"] += 1
                logger.warning(
                    f"⚠️ Centro duplicado por coordenada ignorado | "
                    f"lat={c['lat']} lon={c['lon']} "
                    f"cnpj={c.get('cnpj')} bandeira={c.get('bandeira')}"
                )

        centros = list(uniq.values())

        # reindexa labels
        for i, c in enumerate(centros):
            c["cluster_label"] = i

        stats["validos"] = len(centros)

        # ============================================================
        # 📊 LOG FINAL DE AUDITORIA (Loguru correto)
        # ============================================================
        logger.info(
            f"🏭 Centros | lidos={stats['lidos']} válidos={stats['validos']} "
            f"inv_end={stats['endereco_invalido']} geo_falha={stats['geocode_falha']} "
            f"coord_gen={stats['coord_generica']} dup_coord={stats['duplicado_coord']}"
        )

        return centros

    def _atribuir_pdvs_a_centros(self, pdvs: List[PDV], centros: List[Dict[str, Any]]):
        for p in pdvs:
            melhor = min(
                centros,
                key=lambda c: haversine((p.lat, p.lon), (c["lat"], c["lon"])),
            )
            p.cluster_label = melhor["cluster_label"]

    def _gerar_setores(self, pdvs: List[PDV], centros: List[Dict[str, Any]]) -> List[Setor]:
        setores: List[Setor] = []

        for c in centros:
            label = c["cluster_label"]

            pdvs_cluster = [
                p for p in pdvs if p.cluster_label == label
            ]

            if not pdvs_cluster:
                continue

            metricas = calcular_metricas_cluster(
                pdvs=pdvs_cluster,
                centro_lat=c["lat"],
                centro_lon=c["lon"],
                vel_kmh=self.v_kmh,
            )

            setores.append(
                Setor(
                    cluster_label=label,
                    centro_id=c.get("centro_id"), 
                    centro_lat=c["lat"],
                    centro_lon=c["lon"],
                    n_pdvs=len(pdvs_cluster),
                    raio_med_km=metricas["raio_med_km"],
                    raio_p95_km=metricas["raio_p95_km"],
                    metrics={
                        "distancia_media_km": metricas["distancia_media_km"],
                        "dist_max_km": metricas["dist_max_km"],
                        "tempo_medio_min": metricas["tempo_medio_min"],
                        "tempo_max_min": metricas["tempo_max_min"],
                        "cnpj": c.get("cnpj"),
                        "bandeira": c.get("bandeira"),
                    },

                    subclusters=[],  # mantém compatível
                )
            )

        return setores


