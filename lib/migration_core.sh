#!/bin/bash

BATCH_SIZE="${BATCH_SIZE:-100000}"

get_tables_to_migrate() {
    if [[ "$TABLES" == "all" ]]; then
        psql -d "$SOURCE_DSN" -t -c "
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        " | tr -d ' ' | grep -v '^$'
    else
        echo "$TABLES" | tr ',' '\n'
    fi
}

migrate_table_structure() {
    local table="$1"
    
    log_info "Migrating structure: $table"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would create table $table"
        return 0
    fi
    
    local temp_file="$WORK_DIR/temp/structure_${table}.sql"
    
    pg_dump -s -d "$SOURCE_DSN" -t "$table" > "$temp_file"
    psql -d "$TARGET_DSN" -f "$temp_file" >/dev/null 2>&1 || {
        log_warn "Table $table might already exist"
    }
    
    rm -f "$temp_file"
}

migrate_table_data_copy() {
    local table="$1"
    local thread_id="$2"
    local where_clause="${3:-1=1}"
    
    log_table "[Thread $thread_id] Migrating data: $table"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would migrate $table with: $where_clause"
        return 0
    fi
    
    local temp_file="$WORK_DIR/temp/${table}_${thread_id}.csv"
    local pk_columns=$(get_primary_key_columns "$table")
    
    if [[ -z "$pk_columns" ]]; then
        pk_columns="ctid"  # Fallback для таблиц без PK
    fi
    
    # ЭКСПОРТ через \COPY (БЫСТРО!)
    log_info "Exporting $table data..."
    psql -d "$SOURCE_DSN" -c "\COPY (SELECT * FROM $table WHERE $where_clause ORDER BY $pk_columns) TO STDOUT WITH CSV" > "$temp_file"
    
    # ИМПОРТ через \COPY (БЫСТРО!)
    log_info "Importing $table data..."
    psql -d "$TARGET_DSN" -c "\COPY $table FROM STDIN WITH CSV" < "$temp_file"
    
    # Обработка конфликтов через нашу функцию
    handle_conflicts "$table" "$temp_file"
    
    rm -f "$temp_file"
    log_success "[Thread $thread_id] Completed: $table"
}

handle_conflicts() {
    local table="$1"
    local temp_file="$2"
    
    log_info "Handling conflicts for: $table"
    
    # Здесь можно добавить логику обработки конфликтов
    # используя нашу PL/pgSQL функцию
    local pk_columns=$(get_primary_key_columns "$table")
    
    if [[ -n "$pk_columns" ]]; then
        psql -d "$TARGET_DSN" -c "
            SELECT handle_data_conflicts('$table', '$temp_file', '{$pk_columns}');
        " >/dev/null 2>&1 || true
    fi
}

get_primary_key_columns() {
    local table="$1"
    psql -d "$SOURCE_DSN" -t -c "
        SELECT string_agg(a.attname, ', ' ORDER BY k.ordinal_position)
        FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY k(attnum, ordinal_position) 
            ON k.attnum = a.attnum
        WHERE c.contype = 'p' 
          AND t.relname = '$table' 
          AND n.nspname = 'public'
        GROUP BY c.conname
        LIMIT 1;
    " | tr -d ' ' | grep -v '^$'
}