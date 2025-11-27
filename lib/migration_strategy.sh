#!/bin/bash

migrate_table_with_strategy() {
    local table="$1"
    local thread_id="$2"
    
    case "$MIGRATION_MODE" in
        "full")
            migrate_table_full "$table" "$thread_id"
            ;;
        "incremental")
            migrate_table_incremental "$table" "$thread_id"
            ;;
        "upsert")
            migrate_table_upsert "$table" "$thread_id"
            ;;
        "append")
            migrate_table_append "$table" "$thread_id"
            ;;
        *)
            log_error "Unknown migration mode: $MIGRATION_MODE"
            return 1
            ;;
    esac
}

migrate_table_full() {
    local table="$1"
    local thread_id="$2"
    
    log_table "[FULL] Migrating: $table"
    
    # Очищаем таблицу перед полной миграцией
    log_info "Truncating table: $table"
    psql -d "$TARGET_DSN" -c "TRUNCATE TABLE $table CASCADE;" 2>/dev/null || true
    
    # Мигрируем данные
    migrate_table_data_copy "$table" "$thread_id" "${WHERE_CLAUSES[$table]:-1=1}"
}

migrate_table_incremental() {
    local table="$1"
    local thread_id="$2"
    local where_clause=""
    
    # Автоматически определяем временную колонку если не указана
    local timestamp_column="${INCREMENTAL_COLUMN:-$(detect_timestamp_column "$table")}"
    
    if [[ -n "$LAST_MIGRATION_TIMESTAMP" && -n "$timestamp_column" ]]; then
        where_clause="$timestamp_column > '$LAST_MIGRATION_TIMESTAMP'"
        log_info "Using incremental condition: $where_clause"
    else
        where_clause="${WHERE_CLAUSES[$table]:-1=1}"
    fi
    
    log_table "[INCREMENTAL] Migrating: $table"
    migrate_table_data_copy "$table" "$thread_id" "$where_clause"
}

migrate_table_upsert() {
    local table="$1"
    local thread_id="$2"
    
    log_table "[UPSERT] Migrating: $table"
    
    local temp_file="$WORK_DIR/temp/${table}_${thread_id}.csv"
    local where_clause="${WHERE_CLAUSES[$table]:-1=1}"
    
    # Экспортируем данные
    log_info "Exporting $table data..."
    psql -d "$SOURCE_DSN" -c "\COPY (SELECT * FROM $table WHERE $where_clause) TO STDOUT WITH CSV" > "$temp_file"
    
    # УНИВЕРСАЛЬНЫЙ UPSERT
    log_info "Upserting $table data..."
    upsert_data "$table" "$temp_file"
    
    rm -f "$temp_file"
    log_success "[Thread $thread_id] Completed: $table"
}

migrate_table_append() {
    local table="$1"
    local thread_id="$2"
    
    log_table "[APPEND] Migrating: $table"
    
    # Просто добавляем данные (могут быть дубликаты)
    migrate_table_data_copy "$table" "$thread_id" "${WHERE_CLAUSES[$table]:-1=1}"
}

# УНИВЕРСАЛЬНЫЙ UPSERT для любой таблицы
upsert_data() {
    local table="$1"
    local temp_file="$2"
    
    local pk_columns=$(get_primary_key_columns "$table")
    
    if [[ -z "$pk_columns" ]]; then
        log_warn "No primary key found for $table, using INSERT IGNORE"
        psql -d "$TARGET_DSN" -c "
            CREATE TEMP TABLE temp_upsert AS SELECT * FROM $table WHERE 1=0;
            \copy temp_upsert FROM '$temp_file' WITH CSV;
            
            INSERT INTO $table 
            SELECT * FROM temp_upsert 
            ON CONFLICT DO NOTHING;
            
            DROP TABLE temp_upsert;
        " >/dev/null 2>&1
    else
        # Автоматически генерируем UPDATE часть для всех колонок кроме PK
        local update_columns=$(generate_update_columns "$table" "$pk_columns")
        
        psql -d "$TARGET_DSN" -c "
            CREATE TEMP TABLE temp_upsert AS SELECT * FROM $table WHERE 1=0;
            \copy temp_upsert FROM '$temp_file' WITH CSV;
            
            INSERT INTO $table 
            SELECT * FROM temp_upsert 
            ON CONFLICT ($pk_columns) 
            DO UPDATE SET $update_columns;
            
            DROP TABLE temp_upsert;
        " >/dev/null 2>&1
    fi
}

# Автоматическое определение временных колонок
detect_timestamp_column() {
    local table="$1"
    psql -d "$SOURCE_DSN" -t -c "
        SELECT column_name
        FROM information_schema.columns 
        WHERE table_name = '$table' 
          AND table_schema = 'public'
          AND data_type IN ('timestamp without time zone', 'timestamp with time zone', 'date')
          AND column_name IN ('updated_at', 'modified_at', 'last_updated', 'created_at')
        ORDER BY 
            CASE column_name 
                WHEN 'updated_at' THEN 1
                WHEN 'modified_at' THEN 2  
                WHEN 'last_updated' THEN 3
                WHEN 'created_at' THEN 4
                ELSE 5
            END
        LIMIT 1;
    " | tr -d ' ' | grep -v '^$'
}

# Генерация UPDATE колонок для UPSERT
generate_update_columns() {
    local table="$1"
    local pk_columns="$2"
    
    # Преобразуем PK колонки в массив для исключения
    IFS=',' read -ra pk_array <<< "$pk_columns"
    local exclude_columns=""
    for col in "${pk_array[@]}"; do
        exclude_columns="$exclude_columns|$col"
    done
    
    # Получаем все колонки кроме PK
    psql -d "$SOURCE_DSN" -t -c "
        SELECT string_agg(column_name, ', ')
        FROM information_schema.columns 
        WHERE table_name = '$table' 
          AND table_schema = 'public'
          AND column_name NOT IN (${pk_columns//,/','})
        ORDER BY ordinal_position;
    " | tr -d ' ' | grep -v '^$' | while IFS=',' read -ra columns; do
        local update_set=""
        for col in "${columns[@]}"; do
            if [[ -n "$update_set" ]]; then
                update_set="$update_set, "
            fi
            update_set="${update_set}${col} = EXCLUDED.${col}"
        done
        echo "$update_set"
    done
}