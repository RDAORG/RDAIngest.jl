using DataFrames
using DuckDB
using DBInterface

create_seq(name) = return "CREATE SEQUENCE $name START 1;"

function sourcesql(sequence)
    return """
    CREATE TABLE "sources" (
    "source_id" INTEGER PRIMARY KEY DEFAULT NEXTVAL('$sequence'),
    "name" TEXT NOT NULL
    );
    """
end
function sitesql(sequence)
    return """
    CREATE TABLE "sites" (
    "site_id" INTEGER PRIMARY KEY DEFAULT NEXTVAL('$sequence'),
    "name" TEXT NOT NULL,
    "site_iso_code" TEXT NOT NULL,
    "source_id" INTEGER NOT NULL REFERENCES "sources" ("source_id"),
    UNIQUE ("source_id", "name")
     );
    """
end
file = "D:\\Temp\\test.duckdb"
if isfile(file)
    rm(file)
end
db = DBInterface.connect(DuckDB.DB, file)
try
    DBInterface.execute(db, create_seq("seq_source_id"))
    DBInterface.execute(db, sourcesql("seq_source_id"))
    DBInterface.execute(db, create_seq("seq_site_id"))
    DBInterface.execute(db, sitesql("seq_site_id"))
    DBInterface.close!(db)
finally
    DBInterface.close!(db)
    global db = nothing
    GC.gc() #to ensure database file is released
end
