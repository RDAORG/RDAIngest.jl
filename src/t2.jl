using DuckDB, DBInterface

db = DBInterface.connect(DuckDB.DB,"D:\\Temp\\test.duckdb")
t = DBInterface.execute(db, "SELECT 1")
DBInterface.close!(db)
# db = nothing
# GC.gc()

# db = DBInterface.connect(DuckDB.DB,"D:\\Temp\\test.duckdb")
# t = DBInterface.execute(db, "SELECT 1")
# DBInterface.close!(db)