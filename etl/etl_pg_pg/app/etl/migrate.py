from app.etl.extract import extract_table_data
from app.etl.load import load_table_data

def migrate_table(table_name: str):
    """Мигрирует одну таблицу без трансформаций"""
    print(f"🚀 Начинаем миграцию таблицы: {table_name}")
    
    # Extract
    data, columns = extract_table_data(table_name)
    
    if not data:
        return {"status": "error", "message": "No data found"}
    
    # Load
    success = load_table_data(table_name, data, columns)
    
    if success:
        return {"status": "success", "table": table_name, "rows": len(data)}
    else:
        return {"status": "error", "message": "Load failed"}