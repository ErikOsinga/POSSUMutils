### Script to make a one-shot backup of the prefect databas
### will be saved with timestamp, as e.g. prefect-2025-12-16T142502Z.db


DB="$HOME/.prefect/prefect.db"
OUTDIR="$HOME/prefect-backups"
mkdir -p "$OUTDIR"

ts="$(date -u +%Y-%m-%dT%H%M%SZ)"
out="$OUTDIR/prefect-$ts.db"

sqlite3 "$DB" <<SQL
.timeout 10000
.backup '$out'
SQL

# Optional: verify the backup is sane
sqlite3 "$out" "PRAGMA integrity_check;"
echo "Backup written to: $out"

# Then copy the backup to CANFAR
echo "Copying the backup to CANFAR..."
vcp $out arc:projects/CIRADA/polarimetry/software/prefect-backups



