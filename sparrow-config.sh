unset GREP_OPTIONS
# Configures environment for LaserChron lab

PROJECT_DIR="${0:A:h}"
export SPARROW_BACKUP_DIR="$PROJECT_DIR/database-backups"
export SPARROW_PATH="$PROJECT_DIR/Sparrow"
export SPARROW_LAB_NAME="Arizona LaserChron Center"
export COMPOSE_PROJECT_NAME="laserchron"
# Need to figure out a better way to do this, but it'll be finicky
# because of docker container nonsense
pipeline=$PROJECT_DIR/Sparrow/import-pipelines/LaserChron
export SPARROW_INIT_SQL="$pipeline/sql"
export SPARROW_COMMANDS="$pipeline/bin"
export SPARROW_SITE_CONTENT="$pipeline/site-content"
export SPARROW_HTTP_PORT=80

# Needed for import script
export SPARROW_DATA_DIR="/Data"

override="$PROJECT_DIR/sparrow-config.overrides.sh"
[ -f "$override" ] && source "$override"
