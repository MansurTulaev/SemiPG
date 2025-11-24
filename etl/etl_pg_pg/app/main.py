from fastapi import FastAPI
from app.etl.migrate import migrate_table

app = FastAPI(title="PG to PG ETL")

@app.get("/")
def read_root():
    return {"message": "PG to PG ETL Service"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.post("/migrate/{table_name}")
def migrate_table_endpoint(table_name: str):
    """Мигрирует одну таблицу"""
    result = migrate_table(table_name)
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)