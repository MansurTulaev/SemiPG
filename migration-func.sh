#!/bin/bash

# Перенос схем и таблиц с сохранением структуры
migrate_schema() {
    local source_db="$1"
    local target_db="$2"
    local tables="$3"
    
    IFS=':' read -r source_host source_user source_name source_pass <<< "$source_db"
    IFS=':' read -r target_host target_user target_name target_pass <<< "$target_db"
    
    echo "[SCHEMA] Перенос структуры: $source_name -> $target_name"
    
    local dump_cmd="pg_dump -h '$source_host' -U '$source_user' -d '$source_name' --schema-only --no-owner --no-privileges"
    
    if [ -n "$tables" ]; then
        for table in $tables; do
            dump_cmd="$dump_cmd -t '$table'"
        done
        echo "[SCHEMA] Таблицы: $tables"
    fi
    
    if PGPASSWORD="$source_pass" eval "$dump_cmd" | PGPASSWORD="$target_pass" psql -h "$target_host" -U "$target_user" -d "$target_name" > /dev/null 2>&1; then
        echo "[SCHEMA] Успешно"
        return 0
    else
        echo "[SCHEMA] Ошибка"
        return 1
    fi
}

# Фильтрация и частичный перенос таблиц
migrate_with_filters() {
    local source_db="$1"
    local target_db="$2"
    local table_name="$3"
    local where_condition="$4"
    local order_by="$5"
    local limit_value="$6"
    
    IFS=':' read -r source_host source_user source_name source_pass <<< "$source_db"
    IFS=':' read -r target_host target_user target_name target_pass <<< "$target_db"
    
    echo "[FILTER] Таблица: $table_name"
    echo "  WHERE: ${where_condition:-ALL}"
    echo "  ORDER: ${order_by:-NONE}" 
    echo "  LIMIT: ${limit_value:-ALL}"
    
    local select_query="SELECT * FROM $table_name"
    [ -n "$where_condition" ] && select_query="$select_query WHERE $where_condition"
    [ -n "$order_by" ] && select_query="$select_query ORDER BY $order_by" 
    [ -n "$limit_value" ] && select_query="$select_query LIMIT $limit_value"
    
    local temp_table="temp_migrate_${table_name}_$$"
    local temp_file="/tmp/migrate_data_$$.sql"
    
    if ! PGPASSWORD="$source_pass" psql -h "$source_host" -U "$source_user" -d "$source_name" -c "CREATE TABLE $temp_table AS $select_query;" > /dev/null 2>&1; then
        echo "[FILTER] Ошибка создания таблицы"
        return 1
    fi
    
    if ! PGPASSWORD="$source_pass" pg_dump -h "$source_host" -U "$source_user" -d "$source_name" -t "$temp_table" --data-only --inserts > "$temp_file" 2>/dev/null; then
        echo "[FILTER] Ошибка экспорта"
        PGPASSWORD="$source_pass" psql -h "$source_host" -U "$source_user" -d "$source_name" -c "DROP TABLE $temp_table;" > /dev/null 2>&1
        return 1
    fi
    
    if ! PGPASSWORD="$target_pass" psql -h "$target_host" -U "$target_user" -d "$target_name" -f "$temp_file" > /dev/null 2>&1; then
        echo "[FILTER] Ошибка импорта"
        PGPASSWORD="$source_pass" psql -h "$source_host" -U "$source_user" -d "$source_name" -c "DROP TABLE $temp_table;" > /dev/null 2>&1
        rm -f "$temp_file"
        return 1
    fi
    
    PGPASSWORD="$target_pass" psql -h "$target_host" -U "$target_user" -d "$target_name" -c "ALTER TABLE $temp_table RENAME TO $table_name;" > /dev/null 2>&1

    PGPASSWORD="$source_pass" psql -h "$source_host" -U "$source_user" -d "$source_name" -c "DROP TABLE $temp_table;" > /dev/null 2>&1
    rm -f "$temp_file"
    
    echo "[FILTER] Успешно: $table_name"
    return 0
}

# Пакетный перенос таблиц по конфигурации
migrate_batch_tables() {
    local source_db="$1"
    local target_db="$2"
    local config_file="$3"
    
    if [ ! -f "$config_file" ]; then
        echo "[BATCH] Файл не найден: $config_file"
        return 1
    fi
    
    echo "[BATCH] Запуск пакетного переноса"
    
    local success=0
    local failed=0
    
    while IFS= read -r line; do
        [[ -z "$line" || "$line" =~ ^# ]] && continue
        
        IFS=':' read -r table where order limit <<< "$line"
        
        if migrate_table_with_filters "$source_db" "$target_db" "$table" "$where" "$order" "$limit"; then
            ((success++))
        else
            ((failed++))
        fi
        
    done < "$config_file"
    
    echo "[BATCH] Успешно: $success, Ошибок: $failed"
    [ $failed -eq 0 ] && return 0 || return 1
}

# Проверка подключения к БД
test_db_connection() {
    local db_params="$1"
    IFS=':' read -r host user name pass <<< "$db_params"
    
    if PGPASSWORD="$pass" psql -h "$host" -U "$user" -d "$name" -c "SELECT 1;" > /dev/null 2>&1; then
        echo "Подключение установлено: $name"
        return 0
    else
        echo "Ошибка подключения: $name"
        return 1
    fi
}

export -f migrate_schema
export -f migrate_with_filters
export -f migrate_batch_tables
export -f test_db_connection

# ПРИМЕР ИСПОЛЬЗОВАНИЯ =================
#source ./migration_functions.sh
#migrate_schema "localhost:user:db1:pass" "localhost:user:db2:pass"
#========================================