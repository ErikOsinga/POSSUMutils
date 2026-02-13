### Script to make a one-shot backup of the prefect database
### will be saved with timestamp, as e.g. prefect-2025-12-16T142502Z.sql

# --- PostgreSQL connection settings ---

export PGPASSWORD=prefect   # same password as your Postgres role
PGHOST=postgres
PGPORT=5432
PGUSER=prefect
PGDATABASE=prefect
OUTDIR="$HOME/prefect-backups"
# --- Backup output to host ---
tmp="$1.tmp"

echo "Starting PostgreSQL backup..."
# Run pg_dump inside the container and write output to host path
if ! pg_dump -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$PGDATABASE" > "$tmp"; then
    echo "ERROR: pg_dump failed"
    rm -f "$tmp"
    exit 1
fi

# Move into place only if successful
mv "$tmp" "$1"

echo "Backup written to: $1 on the host"

# -------------------------------
# CLEAN UP OLD BACKUPS
# -------------------------------
# Number of backups to keep locally
MAX_BACKUPS=7
echo "Cleaning up old backups, keeping last $MAX_BACKUPS..."
# List files sorted by creation time, delete all except last $MAX_BACKUPS
ls -1t "$OUTDIR"/prefect-*.sql | tail -n +$((MAX_BACKUPS + 1)) | xargs -r rm -f
echo "Old backups cleanup completed."


