using DuckDB, DBInterface
db = DBInterface.connect(DuckDB.DB, "C:\\Temp\\test.duckdb")
t = DBInterface.execute(db, "SELECT 1")
DBInterface.close!(db)


db = DBInterface.connect(DuckDB.DB, "C:\\Temp\\test.duckdb")
t = DBInterface.execute(db, "SELECT 1")
DBInterface.close!(db)