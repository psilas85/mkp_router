#mkp_router/src/mkp_clusterization/domain/metrics_calculator.py

import numpy as np
from src.mkp_clusterization.domain.haversine_utils import haversine


def calcular_metricas_cluster(pdvs, centro_lat, centro_lon, vel_kmh: float):
    if not pdvs:
        return {
            "distancia_media_km": 0.0,
            "dist_max_km": 0.0,
            "tempo_medio_min": 0.0,
            "tempo_max_min": 0.0,
            "raio_med_km": 0.0,
            "raio_p95_km": 0.0,
        }

    distancias = []

    for p in pdvs:
        if p.lat is None or p.lon is None:
            continue

        d = haversine(
            (p.lat, p.lon),
            (centro_lat, centro_lon)
        )
        distancias.append(d)


    if not distancias:
        return {
            "distancia_media_km": 0.0,
            "dist_max_km": 0.0,
            "tempo_medio_min": 0.0,
            "tempo_max_min": 0.0,
            "raio_med_km": 0.0,
            "raio_p95_km": 0.0,
        }

    dist_arr = np.array(distancias)

    distancia_media = float(dist_arr.mean())
    distancia_max = float(dist_arr.max())

    tempo_medio = (distancia_media / vel_kmh) * 60.0
    tempo_max = (distancia_max / vel_kmh) * 60.0

    return {
        "distancia_media_km": distancia_media,
        "dist_max_km": distancia_max,
        "tempo_medio_min": tempo_medio,
        "tempo_max_min": tempo_max,
        "raio_med_km": distancia_media,
        "raio_p95_km": float(np.percentile(dist_arr, 95)),
    }
