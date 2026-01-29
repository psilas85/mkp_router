#mkp_router/src/mkp_clusterization/domain/entities.py

from dataclasses import dataclass, field
from typing import Optional, Dict, List


@dataclass
class PDV:
    """Representa um ponto de venda (cliente)."""
    id: int
    cnpj: Optional[str]
    nome: Optional[str]
    cidade: Optional[str]
    uf: Optional[str]
    lat: float
    lon: float

    # 🔹 Clusterização
    cluster_label: Optional[int] = None   # rótulo lógico (0..k-1)
    cluster_id: Optional[int] = None      # 🔴 ID real do banco (cluster_setor.id)

    # 🔹 Planejamento operacional
    subcluster_seq: Optional[int] = None  # dia / sequência do vendedor



# ==========================================================
# 🗺️ Entidade Setor (cluster geográfico)
# ==========================================================
@dataclass
class Setor:
    """
    Representa um setor (cluster geográfico).
    Centro é entidade própria (cluster_centro).
    """
    cluster_label: int

    # 🔑 RELAÇÃO COM O CENTRO
    centro_id: int | None

    # 📍 Coordenadas do centro (snapshot)
    centro_lat: float
    centro_lon: float

    # 📊 Métricas principais
    n_pdvs: int
    raio_med_km: float
    raio_p95_km: float

    # 📦 Métricas adicionais / extensíveis
    metrics: Dict[str, float] = field(default_factory=dict)

    # 🔹 Campos opcionais
    pdvs: Optional[List[PDV]] = None
    coords: Optional[List[tuple]] = None

    # 🔹 Hierarquia interna
    subclusters: List[Dict[str, float]] = field(default_factory=list)
