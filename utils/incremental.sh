#!/bin/bash

source "$(dirname "$0")/../lib/logger.sh"
source "$(dirname "$0")/../lib/config_loader.sh"

TIMESTAMP_FILE="last_migration_timestamp"

incremental_migrate() {
    load_config "${1:-config/incremental.conf}"
    
    local last_timestamp=$(get_last_timestamp)
    local current_timestamp=$(date +'%Y-%m-%d %H:%M:%S')
    
    log "Incremental migration from $last_timestamp to $current_timestamp"
    
    # Обновление WHERE условий для инкрементальной миграции
    declare -A incremental_where
    for table in "${!TIMESTAMP_COLUMNS[@]}"; do
        incremental_where["$table"]="${TIMESTAMP_COLUMNS[$table]} > '$last_timestamp'"
    done
    
    # Экспорт для основного скрипта
    export WHERE_CLAUSES="$(declare -p incremental_where)"
    
    # Запуск основной миграции
    "$(dirname "$0")/../main.sh" "$CONFIG_FILE" "${THREADS:-2}"
    
    # Обновление временной метки
    update_timestamp "$current_timestamp"
}

get_last_timestamp() {
    if [[ -f "$TIMESTAMP_FILE" ]]; then
        cat "$TIMESTAMP_FILE"
    else
        echo "1970-01-01 00:00:00"
    fi
}

update_timestamp() {
    echo "$1" > "$TIMESTAMP_FILE"
    log "Updated last migration timestamp: $1"
}