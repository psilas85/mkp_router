#mkp_router/src/mkp_preprocessing/entities/mkp_entity.py

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PDV:
    """
    Entidade PDV (Ponto de Venda)

    Representa um registro de cliente/endereço geolocalizado
    pertencente a um tenant.
    """

    # ============================================================
    # Identificação e endereço
    # ============================================================
    cnpj: str
    logradouro: str
    numero: str
    bairro: str
    cidade: str
    uf: str
    cep: str
    pdv_vendas: Optional[float] = None

    # ============================================================
    # Metadados do processamento
    # ============================================================
    input_id: Optional[str] = None
    descricao: Optional[str] = None

    # ============================================================
    # Endereço completo e cache
    # ============================================================
    pdv_endereco_completo: Optional[str] = None
    endereco_cache_key: Optional[str] = None  # chave canônica (obrigatória no pipeline)

    # ============================================================
    # Dados de geolocalização
    # ============================================================
    pdv_lat: Optional[float] = None
    pdv_lon: Optional[float] = None
    status_geolocalizacao: Optional[str] = None

    # ============================================================
    # Pós-validação geográfica (OFFLINE)
    # ============================================================
    geo_validacao_status: Optional[str] = None   # ok | suspeito | fora_cidade | etc
    geo_validacao_dist_km: Optional[float] = None

    # ============================================================
    # Dados administrativos
    # ============================================================
    tenant_id: Optional[int] = field(default=None)
    id: Optional[int] = field(default=None)
    criado_em: Optional[str] = field(default=None)
    atualizado_em: Optional[str] = field(default=None)

    # ============================================================
    # Pós-init: normalização defensiva
    # ============================================================
    def __post_init__(self):
        # Normaliza strings básicas
        self.cnpj = str(self.cnpj).strip() if self.cnpj is not None else self.cnpj
        self.cep = str(self.cep).strip() if self.cep is not None else self.cep

        if self.endereco_cache_key is not None:
            self.endereco_cache_key = str(self.endereco_cache_key).strip()

        # tenant_id → int
        if self.tenant_id is not None:
            try:
                self.tenant_id = int(self.tenant_id)
            except Exception:
                raise ValueError(f"❌ tenant_id inválido: {self.tenant_id}")

        # Coordenadas → float
        if isinstance(self.pdv_lat, str) and self.pdv_lat.strip():
            self.pdv_lat = float(self.pdv_lat)

        if isinstance(self.pdv_lon, str) and self.pdv_lon.strip():
            self.pdv_lon = float(self.pdv_lon)

        # Distância pós-validação → float
        if isinstance(self.geo_validacao_dist_km, str) and self.geo_validacao_dist_km.strip():
            self.geo_validacao_dist_km = float(self.geo_validacao_dist_km)
