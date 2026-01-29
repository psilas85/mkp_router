#mkp_router/src/mkp_clusterization/domain/centers_geolocation_service.py

import os
import requests
from loguru import logger
from mkp_preprocessing.domain.utils_geo import coordenada_generica


class CentersGeolocationService:

    def __init__(self, reader, writer, google_key: str = None, timeout=7):
        self.reader = reader
        self.writer = writer
        self.google_key = google_key
        self.timeout = timeout

        # usa Nominatim local da EC2
        self.NOMINATIM_URL = os.getenv(
            "NOMINATIM_LOCAL_URL",
            "http://localhost:8080"
        ).rstrip("/") + "/search"

    # ============================================================
    # 🔎 Busca principal
    # ============================================================
    def buscar(self, endereco: str):
        if not endereco or not str(endereco).strip():
            return None, None, "invalido"

        endereco = endereco.strip()

        # 1) CACHE
        lat, lon = self._buscar_cache(endereco)
        if lat and lon and not coordenada_generica(lat, lon):
            return lat, lon, "cache"

        # 2) NOMINATIM LOCAL
        lat, lon = self._buscar_nominatim_local(endereco)
        if lat and lon and not coordenada_generica(lat, lon):
            self._salvar_cache(endereco, lat, lon, origem="nominatim_local")
            return lat, lon, "nominatim_local"

        # 3) GOOGLE (opcional)
        if self.google_key:
            lat, lon = self._buscar_google(endereco)
            if lat and lon and not coordenada_generica(lat, lon):
                self._salvar_cache(endereco, lat, lon, origem="google")
                return lat, lon, "google"

        # 4) FALHA
        return None, None, "falha"

    # ============================================================
    # 📦 Cache
    # ============================================================
    def _buscar_cache(self, endereco):
        try:
            res = self.reader.buscar_endereco_cache(endereco)
            if res:
                return res["lat"], res["lon"]
        except Exception as e:
            logger.warning(f"⚠️ Erro lendo cache: {e}")
        return None, None

    def _salvar_cache(self, endereco, lat, lon, origem):
        try:
            self.writer.salvar_cache(endereco, lat, lon, origem)
        except Exception as e:
            logger.error(f"❌ Falha salvando no cache: {e}")

    # ============================================================
    # 🌍 NOMINATIM LOCAL (EC2)
    # ============================================================
    def _buscar_nominatim_local(self, endereco):
        try:
            params = {
                "q": endereco,
                "format": "json",
                "countrycodes": "br",
                "addressdetails": 1,
                "limit": 1,
            }

            headers = {
                "User-Agent": "mkp-router/1.0 (contato@mkprouter.com)"
            }

            r = requests.get(
                self.NOMINATIM_URL,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )

            if r.status_code != 200:
                return None, None

            dados = r.json()
            if not dados:
                return None, None

            item = dados[0]

            # validação mínima de UF (se existir)
            address = item.get("address", {})
            state = address.get("state", "").lower()
            state_code = address.get("state_code", "").upper()

            # se o endereço menciona SP e o resultado não é SP, rejeita
            if " sp" in endereco.lower() or endereco.lower().endswith("- sp"):
                if state_code and state_code != "SP":
                    return None, None

            return float(item["lat"]), float(item["lon"])

        except Exception as e:
            logger.warning(f"⚠️ Erro Nominatim local: {e}")

        return None, None

    # ============================================================
    # 🗺️ GOOGLE GEOCODING
    # ============================================================
    def _buscar_google(self, endereco):
        try:
            url = (
                "https://maps.googleapis.com/maps/api/geocode/json?"
                f"address={requests.utils.quote(endereco)}&key={self.google_key}"
            )
            dados = requests.get(url, timeout=self.timeout).json()

            if dados.get("status") == "OK":
                loc = dados["results"][0]["geometry"]["location"]
                return loc["lat"], loc["lng"]

        except Exception as e:
            logger.warning(f"⚠️ Erro Google: {e}")

        return None, None
