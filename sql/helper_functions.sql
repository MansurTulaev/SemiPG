-- Функция для безопасной вставки с обработкой конфликтов
CREATE OR REPLACE FUNCTION insert_with_conflict_handler(
    target_table text,
    source_query text,
    conflict_columns text[],
    update_columns text[] DEFAULT NULL
) RETURNS bigint AS $$
DECLARE
    inserted_count bigint := 0;
    conflict_query text;
    update_set text := '';
    col text;
BEGIN
    -- Если указаны колонки для обновления при конфликте
    IF update_columns IS NOT NULL AND array_length(update_columns, 1) > 0 THEN
        FOREACH col IN ARRAY update_columns
        LOOP
            IF update_set != '' THEN
                update_set := update_set || ', ';
            END IF;
            update_set := update_set || format('%I = EXCLUDED.%I', col, col);
        END LOOP;
        
        conflict_query := format(
            'ON CONFLICT (%s) DO UPDATE SET %s',
            array_to_string(conflict_columns, ', '),
            update_set
        );
    ELSE
        -- Игнорировать конфликты
        conflict_query := format(
            'ON CONFLICT (%s) DO NOTHING',
            array_to_string(conflict_columns, ', ')
        );
    END IF;
    
    EXECUTE format(
        'INSERT INTO %I %s %s RETURNING 1',
        target_table,
        source_query,
        conflict_query
    ) INTO inserted_count;
    
    RETURN COALESCE(inserted_count, 0);
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in insert_with_conflict_handler: %', SQLERRM;
        RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- Функция для получения примерных данных о размере таблицы
CREATE OR REPLACE FUNCTION estimate_table_rows(table_name text) 
RETURNS bigint AS $$
DECLARE
    row_count bigint;
BEGIN
    EXECUTE format('SELECT COUNT(*) FROM %I', table_name) INTO row_count;
    RETURN row_count;
EXCEPTION
    WHEN OTHERS THEN
        RETURN 0;
END;
$$ LANGUAGE plpgsql;