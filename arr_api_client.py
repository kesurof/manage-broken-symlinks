#!/usr/bin/env python3
"""
Client API pour Radarr/Sonarr - Refresh, Scan et Search automatiques
"""
import os
import re
import logging
import requests
from typing import Optional, Dict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ArrConfig:
    """Configuration API Radarr/Sonarr"""
    name: str
    category: str
    api_url: str
    api_key: str


class ArrAPIClient:
    """Client générique pour Radarr/Sonarr API v3"""
    
    def __init__(self, config: ArrConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'X-Api-Key': config.api_key,
            'Content-Type': 'application/json'
        })
        self.base_url = config.api_url.rstrip('/')
        
    def _get(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """GET request générique"""
        try:
            url = f"{self.base_url}/api/v3/{endpoint}"
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"[{self.config.name}] GET {endpoint} failed: {e}")
            return None
            
    def _post(self, endpoint: str, data: Dict) -> Optional[Dict]:
        """POST request générique"""
        try:
            url = f"{self.base_url}/api/v3/{endpoint}"
            response = self.session.post(url, json=data, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"[{self.config.name}] POST {endpoint} failed: {e}")
            return None
    
    @staticmethod
    def parse_title_year(folder_name: str) -> tuple[Optional[str], Optional[int]]:
        """
        Extrait titre et année depuis nom de dossier
        Ex: 'The Whale (2022)' -> ('The Whale', 2022)
        """
        match = re.match(r'^(.*)\s+\((\d{4})\)$', folder_name)
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None, None
    
    def find_movie_by_title_year(self, title: str, year: int) -> Optional[Dict]:
        """Recherche film dans Radarr par titre+année"""
        endpoint = "movie"
        items = self._get(endpoint)
        
        if not items:
            return None
            
        for item in items:
            if item.get('title') == title and item.get('year') == year:
                logger.info(f"[{self.config.name}] Trouvé: {title} ({year}) - ID: {item.get('id')}")
                return item
                
        logger.warning(f"[{self.config.name}] Aucun média trouvé pour: {title} ({year})")
        return None
    
    def find_series_by_title(self, title: str) -> Optional[Dict]:
        """Recherche série dans Sonarr par titre (pas d'année pour séries)"""
        endpoint = "series"
        items = self._get(endpoint)
        
        if not items:
            return None
            
        for item in items:
            if item.get('title') == title:
                logger.info(f"[{self.config.name}] Trouvé: {title} - ID: {item.get('id')}")
                return item
                
        logger.warning(f"[{self.config.name}] Aucune série trouvée pour: {title}")
        return None
    
    def refresh_and_scan(self, item_id: int) -> bool:
        """Refresh metadata + Rescan disk"""
        command_name = "RefreshMovie" if self.config.category == "films" else "RefreshSeries"
        id_key = "movieIds" if self.config.category == "films" else "seriesIds"
        
        data = {
            "name": command_name,
            id_key: [item_id]
        }
        
        result = self._post("command", data)
        if result:
            logger.info(f"[{self.config.name}] Refresh+Scan lancé pour ID {item_id} (command_id={result.get('id')})")
            return True
        return False
    
    def search_missing(self, item_id: int) -> bool:
        """Lance recherche automatique pour média manquant"""
        if self.config.category == "films":
            command_name = "MoviesSearch"
            id_key = "movieIds"
        else:
            command_name = "SeriesSearch"
            id_key = "seriesIds"
        
        data = {
            "name": command_name,
            id_key: [item_id]
        }
        
        result = self._post("command", data)
        if result:
            logger.info(f"[{self.config.name}] Recherche lancée pour ID {item_id} (command_id={result.get('id')})")
            return True
        return False
    
    def process_broken_symlink(self, symlink_path: str) -> bool:
        """
        Pipeline complet: Parse → Find → Refresh → Search
        
        Args:
            symlink_path: Chemin complet du symlink cassé
        
        Returns:
            True si traité avec succès
        """
        # Extraire nom du dossier parent
        folder_name = os.path.basename(os.path.dirname(symlink_path))
        
        # Parser titre et année
        title, year = self.parse_title_year(folder_name)
        if not title:
            logger.warning(f"[{self.config.name}] Impossible de parser: {folder_name}")
            return False
        
        # Trouver dans Radarr/Sonarr
        if self.config.category == "films" and year:
            item = self.find_movie_by_title_year(title, year)
        else:
            item = self.find_series_by_title(title)
        
        if not item:
            return False
        
        item_id = item.get('id')
        if not item_id:
            return False
        
        # Refresh + Scan
        if not self.refresh_and_scan(item_id):
            return False
        
        # Recherche automatique
        if not self.search_missing(item_id):
            return False
        
        logger.info(f"[{self.config.name}] Pipeline complet OK pour: {title} ({year if year else 'série'})")
        return True
