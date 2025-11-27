#!/bin/bash

validate_migration() {
    log "Validating migration results..."
    local tables=($(get_tables_to_migrate))
    local total_source=0
    local total_target=0
    local errors=0
    
    for table in "${tables[@]}"; do
        local source_count=$(psql "$SOURCE_DSN" -t -c "SELECT COUNT(*) FROM $table;" | tr -d ' ' | grep -v '^$')
        local target_count=$(psql "$TARGET_DSN" -t -c "SELECT COUNT(*) FROM $table;" | tr -d ' ' | grep -v '^$')
        
        source_count=${source_count:-0}
        target_count=${target_count:-0}
        
        total_source=$((total_source + source_count))
        total_target=$((total_target + target_count))
        
        if [[ "$source_count" -eq "$target_count" ]]; then
            log "✓ Table $table: $source_count = $target_count"
        else
            warn "✗ Table $table: source=$source_count, target=$target_count"
            errors=$((errors + 1))
        fi
    done
    
    # ИСПРАВЛЕННАЯ СТРОКА:
    log "Total records: source=$total_source, target=$total_target"
    
    if [[ $errors -eq 0 ]]; then
        log "✓ Migration validation successful!"
    else
        warn "Migration validation: $errors table(s) have count mismatches"
    fi
    
    return $errors
}