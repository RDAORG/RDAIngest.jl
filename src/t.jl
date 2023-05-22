using DataFrames
using DuckDB
using DBInterface

function sourcesql()
    return raw"""
    CREATE SEQUENCE seq_source_id START 1;
    CREATE TABLE "sources" (
    "source_id" INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_source_id'),
    "name" TEXT NOT NULL
    );
    """
end
function sitesql()
    return raw"""
    CREATE SEQUENCE seq_site_id START 1;
    CREATE TABLE "sites" (
    "site_id" INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_site_id'),
    "name" TEXT NOT NULL,
    "site_iso_code" TEXT NOT NULL,
    "source_id" INTEGER NOT NULL REFERENCES "sources" ("source_id"),
    UNIQUE ("source_id", "name")
     );
    """
end

db = DuckDB.open("D:\\Temp\\test.duckdb")
try
    DuckDB.execute(db, sourcesql())
    DuckDB.execute(db, sitesql())
finally
    DuckDB.close(db)
end
