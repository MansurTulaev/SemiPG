#написал как знал, для общего случая, потом смогу скорректировать под конкретную штучку
import pandas as pd
import time
def migrate(source_conn, target_conn, source_table, target_table,
                  columns='*', where_filter=None, rename_dict=None):
    start = time.time()
    try:
        query = f"SELECT {columns} FROM {source_table}"
        if where_filter:
            query += f" WHERE {where_filter}"
        df = pd.read_sql(query, source_conn)
        if rename_dict:
            df = df.rename(columns=rename_dict)
        df.to_sql(target_table, target_conn, if_exists='replace', index=False)
        print(f"{source_table} -> {target_table}: {len(df)} строк за {time.time() - start:.1f}сек")
        return True
    except Exception as e:
        print(f"Ошибка {source_table}: {e}")
        return False