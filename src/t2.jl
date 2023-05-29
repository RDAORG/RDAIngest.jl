using DuckDB
using DBInterface

db = DuckDB.DB("D:\\Temp\\test.duckdb")
t = DBInterface.execute(db, "SELECT 1")
DuckDB.close_database(db)
