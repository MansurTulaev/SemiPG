#!/bin/bash

# Транзакция на уровне таблицы с чанками
migrate_table_transactional() {
    local table="$1"
    local chunk_size=10000
    
    log_info "Transactional migration: $table"
    
    # Начало транзакции для схемы
    psql "$TARGET_DSN" -c "BEGIN;"
    
    # Создание таблицы
    if ! migrate_table_structure "$table"; then
        psql "$TARGET_DSN" -c "ROLLBACK;"
        return 1
    fi
    
    # Коммит схемы
    psql "$TARGET_DSN" -c "COMMIT;"
    
    # Данные мигрируем чанками (каждый чанк в отдельной транзакции)
    migrate_data_in_chunks "$table" "$chunk_size"
    
    # Индексы создаем CONCURRENTLY вне транзакции
    create_indexes_concurrently "$table"
}

migrate_data_in_chunks() {
    local table="$1"
    local chunk_size="$2"
    local offset=0
    
    while true; do
        # Каждый чанк в своей транзакции
        local migrated=$(psql "$TARGET_DSN" -c "
            BEGIN;
            INSERT INTO $table 
            SELECT * FROM source_$table 
            ORDER BY id LIMIT $chunk_size OFFSET $offset;
            COMMIT;
        " | grep "INSERT" | cut -d' ' -f3)
        
        [[ "$migrated" -eq 0 ]] && break
        offset=$((offset + chunk_size))
        
        log_info "Table $table: migrated $offset rows..."
    done
}