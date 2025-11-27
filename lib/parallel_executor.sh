#!/bin/bash

# НЕ переопределяем SCRIPT_DIR - используем тот, что уже есть из main.sh
# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Используем относительные пути от расположения этого файла
PARALLEL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$PARALLEL_DIR/../utils/progress.sh"
source "$PARALLEL_DIR/../utils/error_handler.sh"

# Глобальные переменные для отслеживания прогресса
PROGRESS_TOTAL=0
PROGRESS_CURRENT=0
declare -A TABLE_PROGRESS

parallel_migrate_tables() {
    local tables=($(get_tables_to_migrate))
    local table_count=${#tables[@]}
    local completed_tables=0
    
    log_stage "PARALLEL TABLE MIGRATION"
    log_info "Migrating $table_count tables using $THREADS threads"
    
    # Инициализируем прогресс-бар
    init_progress "$table_count"
    update_progress 0 "$table_count"
    
    # Массивы для отслеживания процессов
    declare -A job_table
    declare -A job_start_time
    declare -a job_pids
    local running_jobs=0
    
    for table in "${tables[@]}"; do
        # Ждем свободного слота
        while [[ $running_jobs -ge $THREADS ]]; do
            check_running_jobs
            sleep 1
        done
        
        # Мигрируем структуру таблицы
        if ! migrate_table_structure "$table"; then
            log_warning "Failed to migrate structure for: $table"
            continue
        fi
        
        # Запускаем миграцию данных в фоне
        migrate_table_data_copy "$table" "$((completed_tables + 1))" "${WHERE_CLAUSES[$table]:-1=1}" &
        local pid=$!
        
        # Сохраняем информацию о процессе
        job_table["$pid"]="$table"
        job_start_time["$pid"]=$(date +%s)
        job_pids+=("$pid")
        running_jobs=$((running_jobs + 1))
        
        log_info "Started: $table (PID: $pid)"
    done
    
    # Ждем завершения всех процессов
    wait_for_completion
    
    # Финальный прогресс
    update_progress "$table_count" "$table_count"
    show_table_progress
}

check_running_jobs() {
    for pid in "${job_pids[@]}"; do
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            # Процесс завершился
            handle_job_completion "$pid"
            # Удаляем pid из массива
            job_pids=(${job_pids[@]/$pid})
        fi
    done
}

handle_job_completion() {
    local pid=$1
    local table="${job_table[$pid]}"
    local start_time="${job_start_time[$pid]}"
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Ждем завершения процесса и получаем статус
    if wait "$pid"; then
        PROGRESS_CURRENT=$((PROGRESS_CURRENT + 1))
        TABLE_PROGRESS["$table"]="✅ Completed (${duration}s)"
        log_success "Finished: $table (${duration}s)"
    else
        ERROR_COUNT=$((ERROR_COUNT + 1))
        TABLE_PROGRESS["$table"]="❌ Failed (${duration}s)"
        log_error "Failed: $table (${duration}s)"
    fi
    
    # Обновляем прогресс-бар
    update_progress "$PROGRESS_CURRENT" "$PROGRESS_TOTAL"
    
    # Очищаем информацию о процессе
    unset job_table["$pid"]
    unset job_start_time["$pid"]
}

wait_for_completion() {
    local timeout=3600  # 1 hour timeout
    local start_wait=$(date +%s)
    
    while [[ ${#job_pids[@]} -gt 0 ]]; do
        check_running_jobs
        
        local current_time=$(date +%s)
        local wait_time=$((current_time - start_wait))
        
        # Таймаут на случай зависших процессов
        if [[ $wait_time -gt $timeout ]]; then
            log_warning "Timeout waiting for jobs, cleaning up..."
            for pid in "${job_pids[@]}"; do
                if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
                    kill "$pid" 2>/dev/null || true
                    TABLE_PROGRESS["${job_table[$pid]}"]="⏰ Timeout"
                    log_error "Killed hanging process: $pid (${job_table[$pid]})"
                fi
            done
            break
        fi
        
        sleep 2
    done
}

get_running_jobs() {
    local count=0
    for pid in "${job_pids[@]}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            count=$((count + 1))
        fi
    done
    echo $count
}