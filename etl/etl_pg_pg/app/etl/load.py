from app.config import DatabaseConfig

def load_table_data(table_name: str, data: list, column_names: list):
    """Загружает данные в целевую таблицу"""
    conn = DatabaseConfig.get_target_connection()
    cursor = conn.cursor()
    
    try:
        # Сначала удаляем старую таблицу (для простоты)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Создаём таблицу
        create_columns = ", ".join([f'"{col}" TEXT' for col in column_names])
        cursor.execute(f'CREATE TABLE "{table_name}" ({create_columns})')
        
        # Вставляем данные
        if data:
            placeholders = ", ".join(["%s"] * len(column_names))
            columns_str = ", ".join([f'"{col}"' for col in column_names])
            insert_query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
            
            cursor.executemany(insert_query, data)
        
        conn.commit()
        print(f"✅ Успешно загружено {len(data)} строк в {table_name}")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при загрузке данных в {table_name}: {e}")
        return False
    finally:
        cursor.close()
        conn.close()