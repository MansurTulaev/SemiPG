#!/bin/bash

PARALLEL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$PARALLEL_DIR/../utils/progress.sh"
source "$PARALLEL_DIR/../utils/error_handler.sh"

PROGRESS_TOTAL=0
PROGRESS_CURRENT=0
declare -A TABLE_PROGRESS

parallel_migrate_tables() {
    local tables=($(get_tables_to_migrate))
    local table_count=${#tables[@]}
    
    log_stage "PARALLEL TABLE MIGRATION - MODE: $MIGRATION_MODE"
    log_info "Migrating $table_count tables using $THREADS threads"
    
    init_progress "$table_count"
    update_progress 0 "$table_count"
    
    local current=0
    for table in "${tables[@]}"; do
        current=$((current + 1))
        
        # Мигрируем структуру
        if ! migrate_table_structure "$table"; then
            log_warning "Failed to migrate structure for: $table"
            continue
        fi
        
        # Запускаем миграцию данных с выбранной стратегией
        migrate_table_with_strategy "$table" "$current" &
        local pid=$!
        
        log_info "Started: $table (PID: $pid)"
        
        # Ограничиваем количество параллельных процессов
        if [[ $current -ge $THREADS ]]; then
            wait "$pid" || log_warning "Process $pid completed with errors"
        fi
        
        update_progress "$current" "$table_count" "$table"
    done
    
    wait
    update_progress "$table_count" "$table_count"
    show_table_progress
}