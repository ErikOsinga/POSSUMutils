### Script to make a one-shot backup of the prefect database
### will be saved with timestamp, as e.g. prefect-2025-12-16T142502Z.sql

# --- PostgreSQL connection settings ---
PGDATABASE="prefect"
PGUSER="prefect"

# --- Backup output to host ---
OUTDIR="$HOME/prefect-backups"
mkdir -p OUTDIR
ts="$(date -u +%Y-%m-%dT%H%M%SZ)"
out="$OUTDIR/prefect-$ts.sql"

echo "Starting PostgreSQL backup..."
# Run pg_dump inside the container and write output to host path
docker compose exec -T postgres pg_dump -U $PGUSER $PGDATABASE > $out

echo "Backup written to: $out on the host"

# --- Copy to CANFAR ---
echo "Copying the backup to CANFAR..."
vcp "$out" arc:projects/CIRADA/polarimetry/software/prefect-backups

echo "Backup completed successfully"

# -------------------------------
# CLEAN UP OLD BACKUPS
# -------------------------------
# Number of backups to keep locally
MAX_BACKUPS=7
echo "Cleaning up old backups, keeping last $MAX_BACKUPS..."
# List files sorted by creation time, delete all except last $MAX_BACKUPS
ls -1t "$OUTDIR"/prefect-*.sql | tail -n +$((MAX_BACKUPS + 1)) | xargs -r rm -f
echo "Old backups cleanup completed."


