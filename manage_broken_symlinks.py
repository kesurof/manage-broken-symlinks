#!/usr/bin/env python3
"""
Scanner et nettoyeur de symlinks cassés pour médiathèque Plex/Decypharr

Détecte les symlinks cassés pointant vers montages Decypharr (AllDebrid)
et permet leur suppression contrôlée.

Usage:
    python manage_broken_symlinks.py                    # Dry-run
    python manage_broken_symlinks.py --execute          # Avec confirmation
    python manage_broken_symlinks.py --execute --yes    # Auto (cron)

Exit codes:
    0 = Aucun symlink cassé détecté
    1 = Erreur technique (montage, config, permissions)
    2 = Cassés détectés mais non supprimés (dry-run ou refus)
    3 = Suppression exécutée avec succès
"""

import argparse
import json
import logging
import logging.handlers
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import yaml

try:
    from arr_api_client import ArrAPIClient, ArrConfig
    API_CLIENT_AVAILABLE = True
except ImportError:
    API_CLIENT_AVAILABLE = False
    print("WARNING: arr_api_client module not available, API actions disabled", file=sys.stderr)


# Import Rich avec fallback gracieux
try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
    from rich.panel import Panel
    from rich.table import Table
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("[WARNING] Rich library not available, using plain output", file=sys.stderr)


# ============================================================================
# DATACLASSES
# ============================================================================

@dataclass
class DebridInstance:
    """Configuration d'une instance Decypharr"""
    name: str
    category: str
    mount_path: Path
    media_folders: List[str]
    api_url: str = ""  # Nouveau
    api_key: str = ""  # Nouveau
    
    def __post_init__(self):
        self.mount_path = Path(self.mount_path)


@dataclass
class ScanResult:
    """Résultat du scan d'une instance"""
    instance: str
    category: str
    total_checked: int
    broken_links: List[Path]
    duration: float
    
    @property
    def broken_count(self) -> int:
        return len(self.broken_links)


@dataclass
class Config:
    """Configuration globale chargée depuis YAML"""
    media_dir: Path
    log_dir: Path
    instances: List[DebridInstance]
    scan_options: Dict
    
    @classmethod
    def from_yaml(cls, path: Path) -> "Config":
        """Charge la config depuis un fichier YAML"""
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        
        instances = [
            DebridInstance(
                name=inst['name'],
                category=inst['category'],
                mount_path=Path(inst['mount_path']),
                media_folders=inst['media_folders'],
                api_url=inst.get('api_url', ''),  # Nouveau
                api_key=inst.get('api_key', '')   # Nouveau
            )
            for inst in data['instances']
        ]
        
        return cls(
            media_dir=Path(data['media_dir']),
            log_dir=Path(data['log_dir']),
            instances=instances,
            scan_options=data.get('scan_options', {})
        )



# ============================================================================
# LOGGING
# ============================================================================

class JSONLogger:
    """Logger JSON structuré (JSONL format)"""
    
    def __init__(self, log_dir: Path, enabled: bool = True):
        self.enabled = enabled
        self.log_file: Optional[Path] = None
        
        if enabled:
            log_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file = log_dir / f"scan_{timestamp}.jsonl"
    
    def log(self, event: str, level: str = "INFO", **kwargs):
        """Écrit un événement JSON"""
        if not self.enabled or not self.log_file:
            return
        
        entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "event": event,
            **kwargs
        }
        
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(entry) + '\n')
    
    def log_broken(self, path: Path, target: Path, instance: str, category: str):
        """Log spécifique pour symlink cassé"""
        self.log(
            event="broken_symlink",
            level="WARNING",
            path=str(path),
            target=str(target),
            instance=instance,
            category=category
        )


def setup_logging(config: Config) -> tuple[logging.Logger, JSONLogger]:
    """Configure le système de logging (console + syslog + JSON)"""
    
    # Logger Python standard
    logger = logging.getLogger('broken-symlinks')
    logger.setLevel(logging.INFO)
    
    # Handler console
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('[%(levelname)s] %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Handler syslog (journald)
    if config.scan_options.get('syslog_logging', True):
        try:
            syslog_handler = logging.handlers.SysLogHandler(
                address='/dev/log',
                facility=logging.handlers.SysLogHandler.LOG_USER
            )
            syslog_ident = config.scan_options.get('syslog_ident', 'broken-symlinks')
            syslog_formatter = logging.Formatter(f'{syslog_ident}: %(message)s')
            syslog_handler.setFormatter(syslog_formatter)
            logger.addHandler(syslog_handler)
        except Exception as e:
            logger.warning(f"Impossible d'activer syslog: {e}")
    
    # Logger JSON
    json_logger = JSONLogger(
        config.log_dir,
        enabled=config.scan_options.get('json_logging', True)
    )
    
    return logger, json_logger


# ============================================================================
# SCANNER
# ============================================================================

class SymlinkScanner:
    """Scanner de symlinks cassés"""
    
    def __init__(self, config: Config, logger: logging.Logger, json_logger: JSONLogger):
        self.config = config
        self.logger = logger
        self.json_logger = json_logger
        self.use_rich = RICH_AVAILABLE and sys.stderr.isatty() and config.scan_options.get('show_progress', True)
        
        if self.use_rich:
            self.console = Console(stderr=True)
    
    def check_prerequisites(self) -> bool:
        """Vérifie que tous les prérequis sont OK"""
        
        if self.use_rich:
            self.console.print("\n[bold cyan]═══ VÉRIFICATIONS PRÉALABLES ═══[/bold cyan]\n")
        else:
            self.logger.info("=== VÉRIFICATIONS PRÉALABLES ===")
        
        # Vérifier media_dir
        if not self.config.media_dir.exists():
            self.logger.error(f"Répertoire médias inexistant: {self.config.media_dir}")
            self.json_logger.log("error", level="ERROR", message="media_dir_missing", path=str(self.config.media_dir))
            return False
        
        # Vérifier chaque montage
        for instance in self.config.instances:
            if not instance.mount_path.exists():
                self.logger.error(f"Montage {instance.name} inexistant: {instance.mount_path}")
                self.json_logger.log("error", level="ERROR", message="mount_missing", instance=instance.name, path=str(instance.mount_path))
                return False
            
            # Vérifier si vraiment monté
            if not os.path.ismount(str(instance.mount_path)):
                self.logger.warning(f"Montage {instance.name} non actif: {instance.mount_path}")
            
            # Vérifier permissions
            if not os.access(str(instance.mount_path), os.R_OK):
                self.logger.error(f"Montage {instance.name} non accessible en lecture: {instance.mount_path}")
                self.json_logger.log("error", level="ERROR", message="mount_permission_denied", instance=instance.name, path=str(instance.mount_path))
                return False
        
        if self.use_rich:
            self.console.print("[green]✓[/green] Montages vérifiés avec succès\n")
        else:
            self.logger.info("Montages vérifiés avec succès")
        
        return True
    
    def scan_instance(self, instance: DebridInstance) -> ScanResult:
        """Scan une instance Decypharr"""
        
        start_time = datetime.now()
        
        if self.use_rich:
            panel = Panel(
                f"[bold]{instance.category.upper()}[/bold] ({instance.name})",
                box=box.ROUNDED,
                style="cyan"
            )
            self.console.print(panel)
            self.console.print(f"Montage cible: [yellow]{instance.mount_path}[/yellow]\n")
        else:
            self.logger.info(f"=== SCAN {instance.category.upper()} ===")
            self.logger.info(f"Montage cible: {instance.mount_path}")
        
        self.json_logger.log(
            "scan_started",
            instance=instance.name,
            category=instance.category,
            mount_path=str(instance.mount_path)
        )
        
        broken_links: List[Path] = []
        total_checked = 0
        
        for folder in instance.media_folders:
            folder_path = self.config.media_dir / folder
            
            if not folder_path.exists():
                self.logger.warning(f"Dossier ignoré (inexistant): {folder_path}")
                continue
            
            # Collecter tous les symlinks
            symlinks = list(folder_path.rglob('*'))
            symlinks = [p for p in symlinks if p.is_symlink()]
            
            if not symlinks:
                if self.use_rich:
                    self.console.print(f"  {folder}/ [dim](aucun symlink)[/dim]")
                else:
                    self.logger.info(f"  {folder}/ (aucun symlink)")
                continue
            
            # Scanner avec barre de progression
            folder_broken = []
            
            if self.use_rich:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    console=self.console
                ) as progress:
                    task = progress.add_task(f"Analyse {folder}/", total=len(symlinks))
                    
                    for link in symlinks:
                        total_checked += 1
                        if self._is_broken_symlink(link, instance.mount_path):
                            folder_broken.append(link)
                            broken_links.append(link)
                        progress.update(task, advance=1)
            else:
                self.logger.info(f"Analyse: {folder}/ ({len(symlinks)} symlinks)")
                for link in symlinks:
                    total_checked += 1
                    if self._is_broken_symlink(link, instance.mount_path):
                        folder_broken.append(link)
                        broken_links.append(link)
            
            # Afficher résultat dossier
            if folder_broken:
                if self.use_rich:
                    self.console.print(f"  [red]✗[/red] {folder}/: {len(folder_broken)} cassé(s) sur {len(symlinks)}")
                    for broken in folder_broken:
                        self.console.print(f"    [red]❌ {broken}[/red]")
                else:
                    self.logger.warning(f"  {folder}/: {len(folder_broken)} cassé(s) sur {len(symlinks)}")
                    for broken in folder_broken:
                        self.logger.warning(f"[CASSÉ] {broken}")
            else:
                if self.use_rich:
                    self.console.print(f"  [green]✓[/green] {folder}/: {len(symlinks)} symlinks OK")
                else:
                    self.logger.info(f"  {folder}/: {len(symlinks)} symlinks OK")
        
        duration = (datetime.now() - start_time).total_seconds()
        
        # Log résultat instance
        if self.use_rich:
            self.console.print(f"\n[bold]Résultat:[/bold] {len(broken_links)} cassé(s) sur {total_checked} symlinks\n")
        else:
            self.logger.info(f"{instance.category.capitalize()}: {len(broken_links)} cassé(s) sur {total_checked}")
        
        self.json_logger.log(
            "scan_completed",
            instance=instance.name,
            category=instance.category,
            total=total_checked,
            broken=len(broken_links),
            duration_seconds=round(duration, 2)
        )
        
        return ScanResult(
            instance=instance.name,
            category=instance.category,
            total_checked=total_checked,
            broken_links=broken_links,
            duration=duration
        )
    
    def _is_broken_symlink(self, path: Path, mount_target: Path) -> bool:
        """Vérifie si un symlink est cassé et pointe vers le bon montage"""
        
        try:
            # Lire la cible du symlink
            target = path.readlink()
            
            # Vérifier si pointe vers le montage cible
            if not str(target).startswith(str(mount_target)):
                return False
            
            # Tester si cassé
            if not path.exists():
                # Logger en JSON
                self.json_logger.log_broken(
                    path=path,
                    target=target,
                    instance="",  # Sera rempli par le contexte appelant
                    category=""
                )
                return True
            
            return False
            
        except (OSError, RuntimeError):
            # Erreur de lecture du symlink
            return False


# ============================================================================
# CLEANER
# ============================================================================

class SymlinkCleaner:
    """Gestionnaire de suppression de symlinks"""
    
    def __init__(self, logger: logging.Logger, json_logger: JSONLogger, use_rich: bool):
        self.logger = logger
        self.json_logger = json_logger
        self.use_rich = use_rich
        
        if use_rich:
            self.console = Console(stderr=True)
    
    def confirm_deletion(self, total: int, auto_yes: bool) -> bool:
        """Demande confirmation pour la suppression"""
        
        if auto_yes:
            self.logger.info("Mode automatique activé (--yes), suppression sans confirmation")
            return True
        
        if self.use_rich:
            self.console.print(f"\n[bold yellow]⚠️  Confirmer la suppression de {total} symlinks cassés?[/bold yellow] [dim](y/N)[/dim]: ", end="")
        else:
            print(f"\n⚠️  Confirmer la suppression de {total} symlinks cassés? [y/N]: ", end="", file=sys.stderr)
        
        response = input().strip().lower()
        
        if response in ['y', 'yes', 'o', 'oui']:
            return True
        else:
            self.logger.info("Suppression annulée par l'utilisateur")
            return False
    
    def delete_symlinks(self, results: List[ScanResult], config: Config) -> int:
        """Supprime les symlinks cassés et déclenche les actions API"""
        if self.use_rich:
            self.console.print("[bold cyan]SUPPRESSION EN COURS[/bold cyan]")
        else:
            self.logger.info("SUPPRESSION EN COURS")
        
        deleted = 0
        failed = 0
        api_triggered = 0
        
        # Préparer les clients API si activés
        api_clients = {}
        enable_api = config.scan_options.get('enable_api_actions', False)
        
        if enable_api and API_CLIENT_AVAILABLE:
            for instance in config.instances:
                if instance.api_url and instance.api_key:
                    api_config = ArrConfig(
                        name=instance.name,
                        category=instance.category,
                        api_url=instance.api_url,
                        api_key=instance.api_key
                    )
                    api_clients[instance.name] = ArrAPIClient(api_config)
                    self.logger.info(f"Client API activé pour: {instance.name}")
        
        # Supprimer et traiter via API
        for result in results:
            api_client = api_clients.get(result.instance)
            
            for link in result.broken_links:
                # 1. Supprimer le symlink
                if self._delete_file(link):
                    deleted += 1
                    
                    # 2. Déclencher actions API si disponible
                    if api_client:
                        try:
                            if api_client.process_broken_symlink(str(link)):
                                api_triggered += 1
                                self.logger.info(f"Actions API déclenchées pour: {link.name}")
                        except Exception as e:
                            self.logger.error(f"Erreur API pour {link}: {e}")
                else:
                    failed += 1
        
        self.logger.info(f"Suppression terminée: {deleted} réussis, {failed} échecs")
        if api_triggered > 0:
            self.logger.info(f"Actions API déclenchées: {api_triggered} médias")
        
        self.json_logger.log("deletion_completed", deleted=deleted, failed=failed, api_triggered=api_triggered)
        
        return deleted if failed == 0 else -1

    
    def _delete_file(self, path: Path) -> bool:
        """Supprime un fichier"""
        
        try:
            if path.is_symlink():
                path.unlink()
                if self.use_rich:
                    self.console.print(f"[green]✓[/green] Supprimé: {path}")
                else:
                    self.logger.info(f"Supprimé: {path}")
                
                self.json_logger.log(
                    "symlink_deleted",
                    path=str(path)
                )
                return True
            else:
                self.logger.warning(f"Ignoré (plus un symlink): {path}")
                return False
                
        except OSError as e:
            self.logger.error(f"Échec suppression: {path} ({e})")
            self.json_logger.log(
                "deletion_failed",
                level="ERROR",
                path=str(path),
                error=str(e)
            )
            return False


# ============================================================================
# MAIN
# ============================================================================

def show_summary(results: List[ScanResult], execute_mode: bool, use_rich: bool):
    """Affiche le résumé final"""
    
    total_checked = sum(r.total_checked for r in results)
    total_broken = sum(r.broken_count for r in results)
    total_duration = sum(r.duration for r in results)
    
    if use_rich:
        console = Console(stderr=True)
        console.print("\n[bold cyan]═══ RÉSUMÉ ═══[/bold cyan]\n")
        
        table = Table(show_header=True, header_style="bold cyan", box=box.SIMPLE)
        table.add_column("Instance", style="yellow")
        table.add_column("Catégorie", style="magenta")
        table.add_column("Analysés", justify="right")
        table.add_column("Cassés", justify="right", style="red")
        
        for result in results:
            table.add_row(
                result.instance,
                result.category,
                str(result.total_checked),
                str(result.broken_count)
            )
        
        console.print(table)
        console.print(f"\n[bold]Total analysés:[/bold] {total_checked} symlinks")
        console.print(f"[bold]Total cassés:[/bold] {total_broken}")
        console.print(f"[bold]Durée:[/bold] {total_duration:.1f}s\n")
        
        if not execute_mode and total_broken > 0:
            console.print("[dim]Pour supprimer ces symlinks:[/dim]")
            console.print("[dim]  python manage_broken_symlinks.py --execute          # avec confirmation[/dim]")
            console.print("[dim]  python manage_broken_symlinks.py --execute --yes    # automatique (cron)[/dim]\n")
    else:
        logger = logging.getLogger('broken-symlinks')
        logger.info("=== RÉSUMÉ ===")
        logger.info(f"Total analysés: {total_checked} symlinks")
        
        for result in results:
            logger.info(f"{result.category.capitalize()}: {result.broken_count} cassé(s) sur {result.total_checked}")
        
        logger.info(f"TOTAL CASSÉS: {total_broken}")
        logger.info(f"Durée: {total_duration:.1f}s")
        
        if not execute_mode and total_broken > 0:
            print("\nPour supprimer ces symlinks:", file=sys.stderr)
            print("  python manage_broken_symlinks.py --execute          # avec confirmation", file=sys.stderr)
            print("  python manage_broken_symlinks.py --execute --yes    # automatique (cron)", file=sys.stderr)


def main():
    """Point d'entrée principal"""
    
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Scanner et nettoyeur de symlinks cassés pour médiathèque Plex/Decypharr"
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help="Mode suppression (avec confirmation)"
    )
    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help="Suppression automatique sans confirmation (pour cron)"
    )
    parser.add_argument(
        '--config',
        type=Path,
        default=Path(__file__).parent / 'config.yaml',
        help="Chemin vers le fichier de configuration (défaut: config.yaml)"
    )
    
    args = parser.parse_args()
    
    # Charger configuration
    try:
        config = Config.from_yaml(args.config)
    except FileNotFoundError:
        print(f"[ERROR] Fichier de configuration introuvable: {args.config}", file=sys.stderr)
        print(f"[ERROR] Créez le fichier config.yaml dans {Path(__file__).parent}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"[ERROR] Erreur lors du chargement de la configuration: {e}", file=sys.stderr)
        return 1
    
    # Setup logging
    logger, json_logger = setup_logging(config)
    
    # Header
    use_rich = RICH_AVAILABLE and sys.stderr.isatty() and config.scan_options.get('show_progress', True)
    
    if use_rich:
        console = Console(stderr=True)
        console.print(Panel.fit(
            "[bold cyan]Scan des symlinks cassés - Médiathèque Plex[/bold cyan]",
            box=box.DOUBLE
        ))
    else:
        logger.info("=== DÉTECTION SYMLINKS CASSÉS ===")
        logger.info(f"Démarrage: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    mode = "SUPPRESSION" if args.execute else "VÉRIFICATION (dry-run)"
    
    if use_rich:
        console.print(f"\n[bold]Mode:[/bold] {mode}\n")
    else:
        logger.info(f"Mode: {mode}")
    
    json_logger.log(
        "scan_session_started",
        mode="execute" if args.execute else "dry-run",
        auto_yes=args.yes
    )
    
    # Vérifications
    scanner = SymlinkScanner(config, logger, json_logger)
    
    if not scanner.check_prerequisites():
        return 1
    
    # Scan toutes les instances
    results: List[ScanResult] = []
    
    for instance in config.instances:
        result = scanner.scan_instance(instance)
        results.append(result)
    
    # Résumé
    show_summary(results, args.execute, use_rich)
    
    total_broken = sum(r.broken_count for r in results)
    
    # Exit si aucun cassé
    if total_broken == 0:
        logger.info("Aucun symlink cassé détecté")
        json_logger.log("scan_session_completed", broken_found=False)
        return 0
    
    # Mode suppression
    if args.execute:
        cleaner = SymlinkCleaner(logger, json_logger, use_rich)
        
        if cleaner.confirm_deletion(total_broken, args.yes):
            deleted_count = cleaner.delete_symlinks(results, config)
            
            if deleted_count >= 0:
                logger.info("Nettoyage terminé avec succès")
                json_logger.log("scan_session_completed", broken_found=True, deleted=True, count=deleted_count)
                return 3
            else:
                logger.error("Erreurs lors de la suppression")
                json_logger.log("scan_session_completed", broken_found=True, deleted=False, errors=True)
                return 1
        else:
            json_logger.log("scan_session_completed", broken_found=True, deleted=False, user_cancelled=True)
            return 2
    else:
        logger.info("Mode dry-run: aucune suppression effectuée")
        json_logger.log("scan_session_completed", broken_found=True, deleted=False, dry_run=True)
        return 2


if __name__ == '__main__':
    sys.exit(main())
