# src/mkp_preprocessing/scripts/validar_geo_por_input.py

import argparse
import json
import os
from shapely.geometry import shape, Point
from shapely.prepared import prep
from collections import defaultdict

from mkp_preprocessing.infrastructure.database_reader import DatabaseReader
from mkp_preprocessing.infrastructure.database_writer import DatabaseWriter


# ============================================================
# 🔤 Normalização básica (compatível com pipeline)
# ============================================================
def norm(txt: str | None) -> str | None:
    if not txt:
        return None
    return (
        txt.strip()
        .upper()
        .replace("Á", "A")
        .replace("Ã", "A")
        .replace("Â", "A")
        .replace("À", "A")
        .replace("É", "E")
        .replace("Ê", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ô", "O")
        .replace("Õ", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )


# ============================================================
# 📦 Carrega e indexa municípios IBGE
# ============================================================
def carregar_municipios(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    municipios = {}

    for feat in data["features"]:
        props = feat["properties"]

        nome = norm(
            props.get("NM_MUN")
            or props.get("nome")
            or props.get("municipio")
        )
        uf = norm(
            props.get("SIGLA_UF")
            or props.get("uf")
        )

        if not nome or not uf:
            continue

        geom = prep(shape(feat["geometry"]))
        municipios[(nome, uf)] = geom

    return municipios


# ============================================================
# 🚀 MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant_id", type=int, required=True)
    parser.add_argument("--input_id", type=str, required=True)
    parser.add_argument(
        "--geojson",
        default="data/ibge/municipios_poligonos.json"
    )

    args = parser.parse_args()

    reader = DatabaseReader()
    writer = DatabaseWriter()

    if not os.path.exists(args.geojson):
        raise FileNotFoundError(f"GeoJSON não encontrado: {args.geojson}")

    print("📦 Carregando municípios IBGE...")
    municipios = carregar_municipios(args.geojson)
    print(f"✅ Municípios carregados: {len(municipios)}")

    print("📥 Buscando PDVs do input...")
    pdvs = reader.listar_pdvs_por_input(
        tenant_id=args.tenant_id,
        input_id=args.input_id
    )

    print(f"🔎 PDVs encontrados: {len(pdvs)}")

    stats = defaultdict(int)

    for pdv in pdvs:
        pdv_id = pdv["id"]
        cidade = norm(pdv["cidade"])
        uf = norm(pdv["uf"])
        lat = pdv["pdv_lat"]
        lon = pdv["pdv_lon"]

        if lat is None or lon is None:
            writer.atualizar_geo_validacao_pdv(
                pdv_id, "coordenada_invalida", None
            )
            stats["coordenada_invalida"] += 1
            continue

        geom = municipios.get((cidade, uf))
        if not geom:
            writer.atualizar_geo_validacao_pdv(
                pdv_id, "municipio_nao_encontrado", None
            )
            stats["municipio_nao_encontrado"] += 1
            continue

        ponto = Point(lon, lat)

        if geom.contains(ponto):
            writer.atualizar_geo_validacao_pdv(
                pdv_id, "ok", 0.0
            )
            stats["ok"] += 1
        else:
            writer.atualizar_geo_validacao_pdv(
                pdv_id, "fora_cidade", None
            )
            stats["fora_cidade"] += 1

    # ========================================================
    # 📊 RESUMO
    # ========================================================
    total = sum(stats.values())

    print("\n📊 RESUMO GEO-VALIDAÇÃO")
    print(f"Total processados: {total}")
    for k, v in stats.items():
        print(f"  {k:<25}: {v}")

    print("\n✅ Validação finalizada.")


if __name__ == "__main__":
    main()
