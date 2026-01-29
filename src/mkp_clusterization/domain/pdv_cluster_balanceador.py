# ============================================================
# 📦 src/mkp_clusterization/domain/pdv_cluster_balanceador.py
# ============================================================

from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
from loguru import logger

from src.mkp_clusterization.domain.haversine_utils import haversine
from src.mkp_clusterization.domain.entities import PDV


# ============================================================
# 🔧 Helpers
# ============================================================

def _tempo_min(dist_km: float, v_kmh: float) -> float:
    if v_kmh <= 0:
        return 999999.0
    return (dist_km / v_kmh) * 60.0


def _dist_pdv_centro_km(p: PDV, c: Dict[str, Any]) -> float:
    return float(haversine((p.lat, p.lon), (c["lat"], c["lon"])))


def _tempo_pdv_centro_min(p: PDV, c: Dict[str, Any], v_kmh: float) -> float:
    return _tempo_min(_dist_pdv_centro_km(p, c), v_kmh)


def _contagens(pdvs: List[PDV]) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    for p in pdvs:
        if p.cluster_label is None:
            continue
        counts[int(p.cluster_label)] = counts.get(int(p.cluster_label), 0) + 1
    return counts


def _centros_por_label(centros: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    return {int(c["cluster_label"]): c for c in centros}


def _pdvs_do_label(pdvs: List[PDV], label: int) -> List[PDV]:
    return [p for p in pdvs if p.cluster_label == label]


def _labels_ativos(centros: List[Dict[str, Any]]) -> List[int]:
    return sorted([int(c["cluster_label"]) for c in centros])


def _vizinhanca_centros(
    centro_ref: Dict[str, Any],
    centros: List[Dict[str, Any]],
    max_merge_km: float,
) -> List[Dict[str, Any]]:
    lat_c = float(centro_ref["lat"])
    lon_c = float(centro_ref["lon"])

    viz = []
    for c in centros:
        if int(c["cluster_label"]) == int(centro_ref["cluster_label"]):
            continue
        d = float(haversine((lat_c, lon_c), (float(c["lat"]), float(c["lon"]))))
        if d <= max_merge_km:
            c2 = dict(c)
            c2["_dist_km_from_ref"] = d
            viz.append(c2)

    viz.sort(key=lambda x: x["_dist_km_from_ref"])
    return viz


def _candidatos_para_pdv(
    p: PDV,
    centros: List[Dict[str, Any]],
    counts: Dict[int, int],
    max_pdv: int,
    tempo_max_min: int,
    v_kmh: float,
    max_merge_km: Optional[float] = None,
    centro_ref: Optional[Dict[str, Any]] = None,
) -> List[Tuple[Dict[str, Any], float, float, int]]:
    """
    Retorna lista de candidatos viáveis para receber PDV:
      (centro, dist_km, tempo_min, capacidade_disp)
    Regras:
      - respeita max_pdv (capacidade)
      - respeita tempo_max_min (PDV -> centro)
      - opcional: restringe a centros dentro de max_merge_km do centro_ref
    """
    cand = []

    for c in centros:
        label = int(c["cluster_label"])
        atual = int(counts.get(label, 0))
        cap = int(max_pdv - atual)
        if cap <= 0:
            continue

        if max_merge_km is not None and centro_ref is not None:
            d_centros = float(haversine(
                (float(centro_ref["lat"]), float(centro_ref["lon"])),
                (float(c["lat"]), float(c["lon"])),
            ))
            if d_centros > max_merge_km:
                continue

        d = _dist_pdv_centro_km(p, c)
        t = _tempo_min(d, v_kmh)
        if t > float(tempo_max_min):
            continue

        cand.append((c, float(d), float(t), cap))

    cand.sort(key=lambda x: (x[2], x[1]))  # primeiro menor tempo, depois menor distância
    return cand


# ============================================================
# ⚖️ Balanceador principal (CEP-inspired)
# ============================================================

def balancear_clusters_pdv(
    pdvs: List[PDV],
    centros: List[Dict[str, Any]],
    min_pdv: int,
    max_pdv: int,
    tempo_max_min: int,
    v_kmh: float,
    max_iter: int = 10,
    max_merge_km: float = 10.0,
) -> Tuple[List[PDV], List[Dict[str, Any]]]:
    """
    Estratégia CEP-inspired (mais precisa):
      1) Resolver EXCESSO: clusters > max_pdv movem PDVs da borda para centros vizinhos com capacidade.
      2) Resolver DÉFICIT: clusters < min_pdv são DESCARTADOS (centro eliminado) e seus PDVs realocados.
      3) Repetir até convergir ou estourar max_iter.

    Centros são FIXOS.
    PDVs mudam de cluster_label.
    Retorna (pdvs_atualizados, centros_sobreviventes).
    """
    if not centros:
        raise ValueError("centros vazio no balanceador.")
    if min_pdv <= 0 or max_pdv <= 0 or min_pdv > max_pdv:
        raise ValueError("min_pdv/max_pdv inválidos.")
    if tempo_max_min <= 0:
        raise ValueError("tempo_max_min inválido.")

    logger.info(
        f"⚖️ [PDV_BAL] start | centros={len(centros)} | pdvs={len(pdvs)} | "
        f"min={min_pdv} max={max_pdv} tempo_max={tempo_max_min} v={v_kmh} max_merge_km={max_merge_km}"
    )

    # garante cluster_label como int nos PDVs (evita None/str)
    for p in pdvs:
        if p.cluster_label is not None:
            p.cluster_label = int(p.cluster_label)

    centros = [dict(c) for c in centros]
    for c in centros:
        c["cluster_label"] = int(c["cluster_label"])

    for it in range(1, max_iter + 1):
        counts = _contagens(pdvs)
        labels_centros = set(_labels_ativos(centros))

        # remove centros que não existem mais nas contagens (ainda mantém; isso não decide viabilidade)
        # só garante consistência:
        for p in pdvs:
            if p.cluster_label not in labels_centros:
                # PDV ficou apontando para centro removido (de iteração anterior)
                # manda para o mais próximo viável (sem restrição de merge)
                cand = _candidatos_para_pdv(
                    p=p,
                    centros=centros,
                    counts=counts,
                    max_pdv=max_pdv,
                    tempo_max_min=tempo_max_min,
                    v_kmh=v_kmh,
                    max_merge_km=None,
                    centro_ref=None,
                )
                if not cand:
                    raise ValueError("Sem candidatos viáveis para realocar PDV após remoção de centro.")
                c_best = cand[0][0]
                p.cluster_label = int(c_best["cluster_label"])
                counts[p.cluster_label] = counts.get(p.cluster_label, 0) + 1

        logger.info(f"🔁 [PDV_BAL] it={it}/{max_iter} | centros={len(centros)}")

        mov_excesso = _resolver_excesso(
            pdvs=pdvs,
            centros=centros,
            min_pdv=min_pdv,
            max_pdv=max_pdv,
            tempo_max_min=tempo_max_min,
            v_kmh=v_kmh,
            max_merge_km=max_merge_km,
        )

        mov_deficit, centros = _resolver_deficit_descartando_centros(
            pdvs=pdvs,
            centros=centros,
            min_pdv=min_pdv,
            max_pdv=max_pdv,
            tempo_max_min=tempo_max_min,
            v_kmh=v_kmh,
            max_merge_km=max_merge_km,
        )

        mov_total = int(mov_excesso + mov_deficit)
        counts2 = _contagens(pdvs)

        acima = sum(1 for lbl, qtd in counts2.items() if qtd > max_pdv)
        abaixo = sum(1 for lbl, qtd in counts2.items() if qtd < min_pdv)

        logger.info(
            f"📌 [PDV_BAL] it={it} mov={mov_total} | acima={acima} abaixo={abaixo} | centros={len(centros)}"
        )

        if mov_total == 0:
            logger.success("✅ [PDV_BAL] convergiu (nenhuma movimentação possível).")
            break

    # limpeza final: remove centros sem PDV
    counts_f = _contagens(pdvs)
    centros_final = [c for c in centros if counts_f.get(int(c["cluster_label"]), 0) > 0]

    logger.success(
        f"🏁 [PDV_BAL] done | centros={len(centros_final)} | pdvs={len(pdvs)}"
    )
    return pdvs, centros_final


# ============================================================
# 🔵 EXCESSO: > max_pdv (move da borda)
# ============================================================

def _resolver_excesso(
    pdvs: List[PDV],
    centros: List[Dict[str, Any]],
    min_pdv: int,
    max_pdv: int,
    tempo_max_min: int,
    v_kmh: float,
    max_merge_km: float,
) -> int:
    counts = _contagens(pdvs)
    centros_map = _centros_por_label(centros)

    acima = sorted(
        [(lbl, qtd) for lbl, qtd in counts.items() if qtd > max_pdv],
        key=lambda x: x[1],
        reverse=True,
    )

    if not acima:
        return 0

    alteracoes = 0

    for lbl, qtd in acima:
        excedente = int(qtd - max_pdv)
        if excedente <= 0:
            continue

        centro_ref = centros_map.get(int(lbl))
        if not centro_ref:
            continue

        # vizinhos: centros próximos para redistribuir (igual CEP)
        vizinhos = _vizinhanca_centros(centro_ref, centros, max_merge_km=max_merge_km)

        if not vizinhos:
            logger.warning(
                f"🚨 [PDV_BAL] excesso | cluster {lbl} qtd={qtd} sem vizinhos até {max_merge_km} km."
            )
            continue

        # PDVs da borda: mais distantes do próprio centro saem primeiro
        pdvs_cluster = _pdvs_do_label(pdvs, int(lbl))
        pdvs_cluster.sort(
            key=lambda p: _dist_pdv_centro_km(p, centro_ref),
            reverse=True
        )

        i = 0
        while excedente > 0 and i < len(pdvs_cluster):
            p = pdvs_cluster[i]
            i += 1

            # candidatos para esse PDV: vizinhos + capacidade + tempo
            cand = _candidatos_para_pdv(
                p=p,
                centros=vizinhos,  # restringe a vizinhos
                counts=counts,
                max_pdv=max_pdv,
                tempo_max_min=tempo_max_min,
                v_kmh=v_kmh,
                max_merge_km=None,
                centro_ref=None,
            )

            if not cand:
                continue

            c_best = cand[0][0]
            old = int(p.cluster_label)
            new = int(c_best["cluster_label"])

            p.cluster_label = new
            counts[old] = counts.get(old, 0) - 1
            counts[new] = counts.get(new, 0) + 1

            excedente -= 1
            alteracoes += 1

        if excedente > 0:
            logger.warning(
                f"⚠️ [PDV_BAL] excesso | cluster {lbl} manteve excedente={excedente} (sem capacidade/tempo em vizinhos)."
            )

    return alteracoes


# ============================================================
# 🔴 DÉFICIT: < min_pdv (DESCARTA centro)
# ============================================================

def _resolver_deficit_descartando_centros(
    pdvs: List[PDV],
    centros: List[Dict[str, Any]],
    min_pdv: int,
    max_pdv: int,
    tempo_max_min: int,
    v_kmh: float,
    max_merge_km: float,
) -> Tuple[int, List[Dict[str, Any]]]:
    counts = _contagens(pdvs)
    centros_map = _centros_por_label(centros)

    # ordena do menor para o maior: elimina primeiro os piores
    abaixo = sorted(
        [(lbl, qtd) for lbl, qtd in counts.items() if qtd < min_pdv],
        key=lambda x: x[1],
    )

    if not abaixo:
        return 0, centros

    alteracoes = 0
    centros_ativos = [dict(c) for c in centros]

    for lbl, qtd in abaixo:
        lbl = int(lbl)
        qtd = int(qtd)

        # se já foi removido antes
        if lbl not in {int(c["cluster_label"]) for c in centros_ativos}:
            continue

        centro_ref = centros_map.get(lbl)
        if not centro_ref:
            continue

        pdvs_cluster = _pdvs_do_label(pdvs, lbl)
        if not pdvs_cluster:
            # remove centro vazio direto
            centros_ativos = [c for c in centros_ativos if int(c["cluster_label"]) != lbl]
            logger.info(f"🗑️ [PDV_BAL] descartado centro {lbl} (sem PDVs).")
            continue

        # candidatos (outros centros) preferencialmente dentro de max_merge_km do centro_ref
        outros_centros = [c for c in centros_ativos if int(c["cluster_label"]) != lbl]
        if not outros_centros:
            raise ValueError("Não é possível descartar o último centro existente.")

        # Tenta realocar PDVs um a um (ordenado por mais distante do centro que será descartado)
        pdvs_cluster.sort(
            key=lambda p: _dist_pdv_centro_km(p, centro_ref),
            reverse=True
        )

        counts_atual = _contagens(pdvs)
        ok_realocar = True

        for p in pdvs_cluster:
            # tenta primeiro dentro do raio de merge
            cand = _candidatos_para_pdv(
                p=p,
                centros=outros_centros,
                counts=counts_atual,
                max_pdv=max_pdv,
                tempo_max_min=tempo_max_min,
                v_kmh=v_kmh,
                max_merge_km=max_merge_km,
                centro_ref=centro_ref,
            )

            # fallback: qualquer centro (se dentro do raio não der)
            if not cand:
                cand = _candidatos_para_pdv(
                    p=p,
                    centros=outros_centros,
                    counts=counts_atual,
                    max_pdv=max_pdv,
                    tempo_max_min=tempo_max_min,
                    v_kmh=v_kmh,
                    max_merge_km=None,
                    centro_ref=None,
                )

            if not cand:
                ok_realocar = False
                break

            c_best = cand[0][0]
            new = int(c_best["cluster_label"])
            p.cluster_label = new
            counts_atual[new] = counts_atual.get(new, 0) + 1
            alteracoes += 1

        if not ok_realocar:
            # não descartamos o centro — revertendo: reatribui de volta ao lbl (estado original)
            for p in pdvs_cluster:
                if p.cluster_label != lbl:
                    # volta
                    p.cluster_label = lbl
                    alteracoes -= 1  # desfaz contagem de "movimentos" dessa tentativa
            logger.warning(
                f"⚠️ [PDV_BAL] deficit | não foi possível descartar centro {lbl} (sem candidatos viáveis p/ realocar todos PDVs)."
            )
            continue

        # se conseguiu realocar todos, remove o centro
        centros_ativos = [c for c in centros_ativos if int(c["cluster_label"]) != lbl]
        logger.info(
            f"🗑️ [PDV_BAL] descartado centro {lbl} (qtd={qtd}) e realocados {len(pdvs_cluster)} PDVs."
        )

    return alteracoes, centros_ativos
