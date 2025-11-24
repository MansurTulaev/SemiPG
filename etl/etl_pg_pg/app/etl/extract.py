from app.config import DatabaseConfig

def extract_table_data(table_name: str):
    """Извлекает все данные из таблицы"""
    conn = DatabaseConfig.get_source_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем данные
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        
        # Получаем названия колонок
        column_names = [desc[0] for desc in cursor.description]
        
        print(f"✅ Извлечено {len(data)} строк из {table_name}")
        return data, column_names
        
    except Exception as e:
        print(f"❌ Ошибка при извлечении данных из {table_name}: {e}")
        return [], []
    finally:
        cursor.close()
        conn.close()