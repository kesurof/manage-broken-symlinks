# Scanner de Symlinks Cassés - Médiathèque Plex/Decypharr

Détecte et nettoie les symlinks cassés pointant vers les montages Decypharr (AllDebrid).

## Installation

```bash
cd /path/to/your/project/directory
```

Installer les dépendances dans le venv :
```bash
source ~/venv/bin/activate
pip install -r requirements.txt
```

## Configuration

1. Copiez le fichier de configuration d'exemple :
```bash
cp config.example.yaml config.yaml
```

2. Éditez `config.yaml` pour ajuster vos chemins et instances :
   - `media_dir` : Votre répertoire de médias
   - `instances` : Vos instances Radarr/Sonarr avec leurs chemins de montage

## Utilisation

### Mode vérification (dry-run)
```bash
python manage_broken_symlinks.py
```

### Mode suppression avec confirmation
```bash
python manage_broken_symlinks.py --execute
```

### Mode suppression automatique (cron/systemd)
```bash
python manage_broken_symlinks.py --execute --yes
```

## Logs

### Console
- Sortie Rich fancy en mode interactif (TTY)
- Sortie plain text en mode non-interactif (cron)

### JSON structuré (JSONL)
Logs dans `logs/scan_YYYYMMDD_HHMMSS.jsonl` :
```bash
cat logs/scan_*.jsonl | jq '.event'
```

### Syslog (journald)
```bash
journalctl --user -t broken-symlinks
```

## Exit codes

- `0` : Aucun symlink cassé
- `1` : Erreur technique
- `2` : Cassés détectés mais non supprimés
- `3` : Suppression réussie

## Intégration systemd

### Service
```bash
cat > ~/.config/systemd/user/cleanup-broken-symlinks.service << 'EOF'
[Unit]
Description=Nettoyage symlinks cassés médiathèque
After=your-mount-points.mount

[Service]
Type=oneshot
WorkingDirectory=/path/to/your/project
ExecStart=/path/to/your/venv/bin/python manage_broken_symlinks.py --execute --yes
SuccessExitStatus=0 3
StandardOutput=journal
StandardError=journal
EOF
```

### Timer quotidien 3h
```bash
cat > ~/.config/systemd/user/cleanup-broken-symlinks.timer << 'EOF'
[Unit]
Description=Nettoyage quotidien symlinks cassés

[Timer]
OnCalendar=daily
OnCalendar=03:00
Persistent=true

[Install]
WantedBy=timers.target
EOF
```

### Activer
```bash
systemctl --user daemon-reload
systemctl --user enable --now cleanup-broken-symlinks.timer
```

## Développement

### Structure du projet
```
├── config.example.yaml    # Configuration d'exemple
├── config.yaml           # Votre configuration (ignorée par Git)
├── manage_broken_symlinks.py
├── requirements.txt
├── logs/                 # Logs (ignorés par Git)
└── README.md
```

### Contribuer
1. Utilisez `config.example.yaml` comme base
2. Ne commitez jamais `config.yaml` ou le contenu de `logs/`
3. Testez en mode dry-run avant soumission