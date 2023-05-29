"""
    createdatabase(path, name)

Creates a SQLite database to store the information contained in the Reference Death Archive (RDA)
"""
function createdatabase(path, name; replace=false, type="sqlite")
    file = joinpath(path, "$name.$type")
    existed = isfile(file)
    if existed && !replace
        error("Database '$file' already exists.")
    end
    if existed && replace
        GC.gc() #to ensure database file is released
        rm(file)
    end
    if !existed && !isdir(path)
        mkpath(path)
    end
    if lowercase(type) == "sqlite"
        global db = SQLite.DB(file)
    elseif lowercase(type) == "duckdb"
        global db = DuckDB.open(file)
    else
        error("Unknown database type '$type'")
    end
    try
        createsources(db)
        createprotocols(db)
        createtransformations(db)
        createvariables(db)
        createdatasets(db)
        createinstruments(db)
        createdeaths(db)
    finally
        if lowercase(type) == "sqlite"
            close(db)
        elseif lowercase(type) == "duckdb"
            DBInterface.close!(db)
            DuckDB.close_database(db)
            close(db)
        end
        global db = nothing
    end
end
"""
    opendatabase(path::String, name::String)::DBInterface.Connection

Open file on path as an SQLite database (assume .sqlite extension)
"""
function opendatabase(path::String, name::String; type="sqlite")::DBInterface.Connection
    file = joinpath(path, "$name.$type")
    if isfile(file)
        if lowercase(type) == "sqlite"
            return SQLite.DB(file)
        elseif lowercase(type) == "duckdb"
            return DuckDB.open(file)
        else
            error("Unknown database type '$type'")
        end
        error("File '$file' not found.")
    end
end
"""
    get_table(db::DBInterface.Connection, table::String)::AbstractDataFrame

Retrieve table `table` as a DataFrame from `db`
"""
function get_table(db::DBInterface.Connection, table::String)::AbstractDataFrame
    sql = "SELECT * FROM $(table)"
    df = DBInterface.execute(db, sql; iterate_rows=true) |> DataFrame
    return df
end
"""
    createsources(db::SQLite.DB)


    createsources(db::DBInterface.Connection)

Creates tables to record a source and associated site/s for deaths contributed to the RDA
"""
function createsources(db::SQLite.DB)
    DBInterface.executemultiple(db, sourcesqlite())
    DBInterface.executemultiple(db, sitesqlite())
    return nothing
end
create_seq(name) = return "CREATE SEQUENCE $name START 1;"
function createsources(db::DuckDB.DB)
    DBInterface.execute(db, create_seq("seq_source_id"))
    DBInterface.execute(db, sourcesqlduck("seq_source_id"))
    DBInterface.execute(db, create_seq("seq_site_id"))
    DBInterface.execute(db, sitesqlduck("seq_site_id"))
    return nothing
end
function sourcesqlite()
    return raw"""
    CREATE TABLE "sources" (
    "source_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL
    );
    """
end
function sourcesqlduck(sequence)
    return """
    CREATE TABLE "sources" (
    "source_id" INTEGER PRIMARY KEY DEFAULT NEXTVAL('$sequence'),
    "name" TEXT NOT NULL
    );
    """
end
function sitesqlite()
    return raw"""
    CREATE TABLE "sites" (
    "site_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "site_iso_code" TEXT NOT NULL,
    "source_id" INTEGER NOT NULL,
    CONSTRAINT "fk_sites_source_id" FOREIGN KEY ("source_id") 
      REFERENCES "sources" ("source_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    CREATE UNIQUE INDEX "i_source_name"
    ON "sites" (
    "source_id" ASC,
    "name" ASC
    );
    """
end
function sitesqlduck(sequence)
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
"""
    createprotocols(db::DBInterface.Connection)

Create tables to record information about protocols and the ethics approvals for those protocols
"""
function createprotocols(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "ethics" (
    "ethics_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "ethics_committee" TEXT NOT NULL,
    "ethics_reference" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "ethics_documents" (
    "ethics_document_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "ethics_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "document" BLOB,
    CONSTRAINT "fk_ethics_documents_ethics_id" FOREIGN KEY ("ethics_id") REFERENCES "ethics" ("ethics_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_ethic_name"
    ON "ethics_documents" (
    "ethics_id" ASC,
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "protocols" (
    "protocol_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "ethics_id" INTEGER,
    CONSTRAINT "fk_protocols_ethics_id" FOREIGN KEY ("ethics_id") REFERENCES "ethics" ("ethics_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_ethics_name"
    ON "protocols" (
    "ethics_id" ASC,
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "site_protocols" (
    "site_id" INTEGER NOT NULL,
    "protocol_id" INTEGER NOT NULL,
    PRIMARY KEY ("site_id", "protocol_id"),
    CONSTRAINT "fk_site_protocols_site_id" FOREIGN KEY ("site_id") REFERENCES "sites" ("site_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_site_protocols_protocol_id" FOREIGN KEY ("protocol_id") REFERENCES "protocols" ("protocol_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "protocol_documents" (
    "protocol_document_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "protocol_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "document" BLOB,
    CONSTRAINT "fk_protocol_documents_protocol_id" FOREIGN KEY ("protocol_id") REFERENCES "protocols" ("protocol_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
function createprotocols(db::DuckDB.DB)
    DBInterface.execute(db, create_seq("seq_ethics_id"))
    sql = """
    CREATE TABLE "ethics" (
    "ethics_id" INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_ethics_id'),
    "name" TEXT NOT NULL,
    "ethics_committee" TEXT NOT NULL,
    "ethics_reference" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_ethics_document_id"))
    sql = raw"""
    CREATE TABLE "ethics_documents" (
    "ethics_document_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_ethics_document_id'),
    "ethics_id" INTEGER NOT NULL REFERENCES "ethics" ("ethics_id"),
    "name" TEXT NOT NULL,
    "document" BLOB,
    UNIQUE ("ethics_id", "name")
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_protocol_id"))
    sql = raw"""
    CREATE TABLE "protocols" (
    "protocol_id" INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_protocol_id'),
    "name" TEXT NOT NULL,
    "ethics_id" INTEGER REFERENCES "ethics" ("ethics_id"),
    UNIQUE("ethics_id", "name")
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "site_protocols" (
    "site_id" INTEGER NOT NULL REFERENCES "sites" ("site_id"),
    "protocol_id" INTEGER NOT NULL REFERENCES "protocols" ("protocol_id"),
    PRIMARY KEY ("site_id", "protocol_id")
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_protocol_document_id"))
    sql = raw"""
    CREATE TABLE "protocol_documents" (
    "protocol_document_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_protocol_document_id'),
    "protocol_id" INTEGER NOT NULL REFERENCES "protocols" ("protocol_id"),
    "name" TEXT NOT NULL,
    "document" BLOB
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
"""
    createtransformations(db)

Create tables to record data transformations and data ingests
"""
function createtransformations(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "transformation_types" (
    "transformation_type_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformation_statuses" (
    "transformation_status_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformations" (
    "transformation_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "transformation_type_id" INTEGER NOT NULL,
    "transformation_status_id" INTEGER NOT NULL,
    "description" TEXT NOT NULL,
    "code_reference" TEXT NOT NULL,
    "date_created" DATE NOT NULL,
    "created_by" TEXT NOT NULL,
    CONSTRAINT "fk_transformations_transformation_type_id" FOREIGN KEY ("transformation_type_id") REFERENCES "transformation_types" ("transformation_type_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_transformations_transformation_status_id" FOREIGN KEY ("transformation_status_id") REFERENCES "transformation_statuses" ("transformation_status_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "data_ingestions" (
    "data_ingestion_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "source_id" INTEGER NOT NULL,
    "date_received" DATE NOT NULL,
    "description" TEXT,
    CONSTRAINT "fk_data_ingestions_source_id" FOREIGN KEY ("source_id") REFERENCES "sources" ("source_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    types = DataFrame([(transformation_type_id=1, name="Raw data ingest"), (transformation_type_id=2, name="Dataset transform")])
    statuses = DataFrame([(transformation_status_id=1, name="Unverified"), (transformation_status_id=2, name="Verified")])
    SQLite.load!(types, db, "transformation_types")
    SQLite.load!(statuses, db, "transformation_statuses")
    return nothing
end
function createtransformations(db::DuckDB.DB)
    DBInterface.execute(db, create_seq("seq_transformation_type_id"))
    sql = raw"""
    CREATE TABLE "transformation_types" (
    "transformation_type_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_transformation_type_id'),
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_transformation_status_id"))
    sql = raw"""
    CREATE TABLE "transformation_statuses" (
    "transformation_status_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_transformation_status_id'),
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, create_seq("seq_transformation_id"))
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformations" (
    "transformation_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_transformation_id'),
    "transformation_type_id" INTEGER NOT NULL REFERENCES "transformation_types" ("transformation_type_id"),
    "transformation_status_id" INTEGER NOT NULL REFERENCES "transformation_statuses" ("transformation_status_id"),
    "description" TEXT NOT NULL,
    "code_reference" TEXT NOT NULL,
    "date_created" DATE NOT NULL,
    "created_by" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, create_seq("seq_data_ingestion_id"))
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "data_ingestions" (
    "data_ingestion_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_data_ingestion_id'),
    "source_id" INTEGER NOT NULL REFERENCES "sources" ("source_id"),
    "date_received" DATE NOT NULL,
    "description" TEXT
     );
    """
    DBInterface.execute(db, sql)
    types = DataFrame([(transformation_type_id=1, name="Raw data ingest"), (transformation_type_id=2, name="Dataset transform")])
    statuses = DataFrame([(transformation_status_id=1, name="Unverified"), (transformation_status_id=2, name="Verified")])
    DuckDB.appendDataFrame(types, db, "transformation_types")
    DuckDB.appendDataFrame(statuses, db, "transformation_statuses")
    return nothing
end
"""
    createvariables(db)

Create tables to record value types, variables and vocabularies
"""
function createvariables(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "value_types" (
    "value_type_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "value_type" TEXT NOT NULL,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_value_type"
    ON "value_types" (
    "value_type" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "vocabularies" (
    "vocabulary_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL UNIQUE,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "vocabulary_items" (
    "vocabulary_item_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "vocabulary_id" INTEGER NOT NULL,
    "value" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "description" TEXT,
    CONSTRAINT "fk_vocabulary_items" FOREIGN KEY ("vocabulary_id") REFERENCES "vocabularies"("vocabulary_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "domains" (
    "domain_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "description" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_domain_name"
    ON "domains" (
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "variables" (
    "variable_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "domain_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "value_type_id" INTEGER NOT NULL,
    "vocabulary_id" INTEGER,
    "description" TEXT,
    "note" TEXT,
    CONSTRAINT "fk_variables_domain_id" FOREIGN KEY ("domain_id") REFERENCES "domains"("domain_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_variables_value_type_id" FOREIGN KEY ("value_type_id") REFERENCES "value_types"("value_type_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_variables_vocabulary_id" FOREIGN KEY ("vocabulary_id") REFERENCES "vocabularies"("vocabulary_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_variables_domain_name"
    ON "variables" (
    "domain_id" ASC,
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "vocabulary_mapping" (
    "vocabulary_mapping_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "from_vocabulary_item" INTEGER NOT NULL,
    "to_vocabulary_item" INTEGER NOT NULL,
    CONSTRAINT "fk_vocabulary_mapping" FOREIGN KEY ("from_vocabulary_item") REFERENCES "vocabulary_items" ("vocabulary_item_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_vocabulary_mapping" FOREIGN KEY ("to_vocabulary_item") REFERENCES "vocabulary_items" ("vocabulary_item_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    types = DataFrame([(value_type_id=1, value_type="Integer", description=""),
        (value_type_id=2, value_type="Float", description=""),
        (value_type_id=3, value_type="String", description=""),
        (value_type_id=4, value_type="Date", description="ISO Date yyyy-mm-dd"),
        (value_type_id=5, value_type="Datetime", description="ISO Datetime yyyy-mm-ddTHH:mm:ss.sss"),
        (value_type_id=6, value_type="Time", description="ISO Time HH:mm:ss.sss"),
        (value_type_id=7, value_type="Categorical", description="Category represented by a Vocabulary with integer value and string code, stored as Integer")
    ])
    SQLite.load!(types, db, "value_types")
    return nothing
end
function createvariables(db::DuckDB.DB)
    DBInterface.execute(db, create_seq("seq_value_type_id"))
    sql = raw"""
    CREATE TABLE "value_types" (
    "value_type_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_value_type_id'),
    "value_type" TEXT NOT NULL UNIQUE,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_vocabulary_id"))
    sql = raw"""
    CREATE TABLE "vocabularies" (
    "vocabulary_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_vocabulary_id'),
    "name" TEXT NOT NULL UNIQUE,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_vocabulary_item_id"))
    sql = raw"""
    CREATE TABLE "vocabulary_items" (
    "vocabulary_item_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_vocabulary_item_id'),
    "vocabulary_id" INTEGER NOT NULL REFERENCES "vocabularies"("vocabulary_id"),
    "value" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_domain_id"))
    sql = raw"""
    CREATE TABLE "domains" (
    "domain_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_domain_id'),
    "name" TEXT NOT NULL UNIQUE,
    "description" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_variable_id"))
    sql = raw"""
    CREATE TABLE "variables" (
    "variable_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_variable_id'),
    "domain_id" INTEGER NOT NULL REFERENCES "domains"("domain_id"),
    "name" TEXT NOT NULL,
    "value_type_id" INTEGER NOT NULL REFERENCES "value_types"("value_type_id"),
    "vocabulary_id" INTEGER REFERENCES "vocabularies"("vocabulary_id"),
    "description" TEXT,
    "note" TEXT,
    UNIQUE ("domain_id", "name")
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_vocabulary_mapping_id"))
    sql = raw"""
    CREATE TABLE "vocabulary_mapping" (
    "vocabulary_mapping_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_vocabulary_mapping_id'),
    "from_vocabulary_item" INTEGER NOT NULL REFERENCES "vocabulary_items" ("vocabulary_item_id"),
    "to_vocabulary_item" INTEGER NOT NULL REFERENCES "vocabulary_items" ("vocabulary_item_id")
    );
    """
    DBInterface.execute(db, sql)
    types = DataFrame([(value_type_id=1, value_type="Integer", description=""),
        (value_type_id=2, value_type="Float", description=""),
        (value_type_id=3, value_type="String", description=""),
        (value_type_id=4, value_type="Date", description="ISO Date yyyy-mm-dd"),
        (value_type_id=5, value_type="Datetime", description="ISO Datetime yyyy-mm-ddTHH:mm:ss.sss"),
        (value_type_id=6, value_type="Time", description="ISO Time HH:mm:ss.sss"),
        (value_type_id=7, value_type="Categorical", description="Category represented by a Vocabulary with integer value and string code, stored as Integer")
    ])
    DuckDB.appendDataFrame(types, db, "value_types")
end

"""
    createdatasets(db::DBInterface.Connection)

Create tables to record datasets, rows, data and links to the transformations that use/created the datasets
"""
function createdatasets(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "datasets" (
    "dataset_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "date_created" DATE NOT NULL,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "datarows" (
    "row_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "dataset_id" INTEGER NOT NULL,
    CONSTRAINT "fk_datarows_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "data" (
    "row_id" INTEGER NOT NULL,
    "variable_id" INTEGER NOT NULL,
    "value" TEXT NULL,
    PRIMARY KEY ("row_id", "variable_id"),
    CONSTRAINT "fk_data_row_id" FOREIGN KEY ("row_id") REFERENCES "datarows" ("row_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_data_variable_id" FOREIGN KEY ("variable_id") REFERENCES "variables" ("variable_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "unique_data" UNIQUE ("row_id" ASC, "variable_id" ASC)
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformation_inputs" (
    "transformation_id" INTEGER NOT NULL,
    "dataset_id" INTEGER NOT NULL,
    PRIMARY KEY ("transformation_id", "dataset_id"),
    CONSTRAINT "fk_transformation_inputs_transformation_id" FOREIGN KEY ("transformation_id") REFERENCES "transformations" ("transformation_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_transformation_inputs_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformation_outputs" (
    "transformation_id" INTEGER NOT NULL,
    "dataset_id" INTEGER NOT NULL,
    PRIMARY KEY ("transformation_id", "dataset_id"),
    CONSTRAINT "fk_transformation_outputs_transformation_id" FOREIGN KEY ("transformation_id") REFERENCES "transformations" ("transformation_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_transformation_outputs_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "dataset_variables" (
    "dataset_id" INTEGER NOT NULL,
    "variable_id" INTEGER NOT NULL,
    PRIMARY KEY ("dataset_id", "variable_id"),
    CONSTRAINT "fk_dataset_variables_variable_id" FOREIGN KEY ("variable_id") REFERENCES "variables" ("variable_id") ON DELETE NO ACTION ON UPDATE RESTRICT,
    CONSTRAINT "fk_dataset_variables_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "ingest_datasets" (
        ingest_dataset_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        data_ingestion_id INTEGER NOT NULL,
        transformation_id INTEGER NOT NULL,
        dataset_id INTEGER NOT NULL,
        CONSTRAINT "fk_ingest_datasets_data_ingestion_id" FOREIGN KEY ("data_ingestion_id") REFERENCES "data_ingestions" ("data_ingestion_id") ON DELETE CASCADE ON UPDATE RESTRICT,
        CONSTRAINT "fk_ingest_datasets_transformation_id" FOREIGN KEY ("transformation_id") REFERENCES "transformations" ("transformation_id") ON DELETE CASCADE ON UPDATE RESTRICT
        CONSTRAINT "fk_ingest_datasets_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE NO ACTION ON UPDATE RESTRICT
    )
    """
    DBInterface.execute(db, sql)
    return nothing
end
function createdatasets(db::DuckDB.DB)
    DBInterface.execute(db, create_seq("seq_dataset_id"))
    sql = raw"""
    CREATE TABLE "datasets" (
    "dataset_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_dataset_id'),
    "name" TEXT NOT NULL,
    "date_created" DATE NOT NULL,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_row_id"))
    sql = raw"""
    CREATE TABLE "datarows" (
    "row_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_row_id'),
    "dataset_id" INTEGER NOT NULL REFERENCES "datasets" ("dataset_id")
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "data" (
    "row_id" INTEGER NOT NULL REFERENCES "datarows" ("row_id"),
    "variable_id" INTEGER NOT NULL REFERENCES "variables" ("variable_id"),
    "value" TEXT NULL,
    PRIMARY KEY ("row_id", "variable_id"),
    UNIQUE ("row_id", "variable_id")
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformation_inputs" (
    "transformation_id" INTEGER NOT NULL REFERENCES "transformations" ("transformation_id"),
    "dataset_id" INTEGER NOT NULL REFERENCES "datasets" ("dataset_id"),
    PRIMARY KEY ("transformation_id", "dataset_id")
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformation_outputs" (
    "transformation_id" INTEGER NOT NULL REFERENCES "transformations" ("transformation_id"),
    "dataset_id" INTEGER NOT NULL REFERENCES "datasets" ("dataset_id"),
    PRIMARY KEY ("transformation_id", "dataset_id")
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "dataset_variables" (
    "dataset_id" INTEGER NOT NULL REFERENCES "variables" ("variable_id"),
    "variable_id" INTEGER NOT NULL REFERENCES "datasets" ("dataset_id"),
    PRIMARY KEY ("dataset_id", "variable_id")
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_ingest_dataset_id"))
    sql = raw"""
    CREATE TABLE "ingest_datasets" (
        ingest_dataset_id INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_ingest_dataset_id'),
        data_ingestion_id INTEGER NOT NULL REFERENCES "data_ingestions" ("data_ingestion_id"),
        transformation_id INTEGER NOT NULL REFERENCES "transformations" ("transformation_id"),
        dataset_id INTEGER NOT NULL REFERENCES "datasets" ("dataset_id")
    )
    """
    DBInterface.execute(db, sql)
    return nothing
end
"""
    createinstruments(db::DBInterface.Connection)

Create tables to record data collection instruments, and their associated protocols and datasets
"""
function createinstruments(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "instruments" (
    "instrument_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_instrument_name"
    ON "instruments" (
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "instrument_datasets" (
    "instrument_id" INTEGER NOT NULL,
    "dataset_id" INTEGER NOT NULL,
    PRIMARY KEY ("instrument_id", "dataset_id"),
    CONSTRAINT "fk_instrument_datasets_instrument_id" FOREIGN KEY ("instrument_id") REFERENCES "instruments" ("instrument_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_instrument_datasets_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "instrument_documents" (
    "intrument_document_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "instrument_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "document" BLOB,
    CONSTRAINT "fk_instrument_documents_instrument_id" FOREIGN KEY ("instrument_id") REFERENCES "instruments" ("instrument_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "u_instrument_documents" UNIQUE ("instrument_id" ASC, "name" ASC)
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "protocol_instruments" (
    "protocol_id" INTEGER NOT NULL,
    "instrument_id" INTEGER NOT NULL,
    PRIMARY KEY ("protocol_id", "instrument_id"),
    CONSTRAINT "fk_protocol_instruments_protocol_id" FOREIGN KEY ("protocol_id") REFERENCES "protocols" ("protocol_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_protocol_instruments" FOREIGN KEY ("instrument_id") REFERENCES "instruments" ("instrument_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
function createinstruments(db::DuckDB.DB)
    DBInterface.execute(db, create_seq("seq_instrument_id"))
    sql = raw"""
    CREATE TABLE "instruments" (
    "instrument_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_instrument_id'),
    "name" TEXT NOT NULL UNIQUE
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "instrument_datasets" (
    "instrument_id" INTEGER NOT NULL REFERENCES "instruments" ("instrument_id"),
    "dataset_id" INTEGER NOT NULL REFERENCES "datasets" ("dataset_id"),
    PRIMARY KEY ("instrument_id", "dataset_id")
    );
    """
    DBInterface.execute(db, sql)
    DBInterface.execute(db, create_seq("seq_intrument_document_id"))
    sql = raw"""
    CREATE TABLE "instrument_documents" (
    "intrument_document_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_intrument_document_id'),
    "instrument_id" INTEGER NOT NULL REFERENCES "instruments" ("instrument_id"),
    "name" TEXT NOT NULL,
    "document" BLOB,
    UNIQUE ("instrument_id", "name")
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "protocol_instruments" (
    "protocol_id" INTEGER NOT NULL REFERENCES "protocols" ("protocol_id"),
    "instrument_id" INTEGER NOT NULL REFERENCES "instruments" ("instrument_id"),
    PRIMARY KEY ("protocol_id", "instrument_id")
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
"""
    createdeaths(db)

Create tables to store deaths, and their association with data rows and data ingests
"""
function createdeaths(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "deaths" (
    "death_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "site_id" INTEGER NOT NULL,
    "external_id" TEXT NOT NULL,
    "data_ingestion_id" INTEGER NOT NULL,
    CONSTRAINT "fk_deaths_site_id" FOREIGN KEY ("site_id") REFERENCES "sites" ("site_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_deaths_data_ingestion_id" FOREIGN KEY ("data_ingestion_id") REFERENCES "data_ingestions" ("data_ingestion_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "unique_external_id" UNIQUE ("site_id" ASC, "external_id" ASC)
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE INDEX "i_deaths_site_id"
    ON "deaths" (
    "site_id" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "death_rows" (
    "death_id" INTEGER NOT NULL,
    "row_id" INTEGER NOT NULL,
    PRIMARY KEY ("death_id", "row_id"),
    CONSTRAINT "fk_death_rows_death_id" FOREIGN KEY ("death_id") REFERENCES "deaths" ("death_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_death_rows_row_id" FOREIGN KEY ("row_id") REFERENCES "datarows" ("row_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "unique_rows" UNIQUE ("death_id" ASC, "row_id" ASC)
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
function createdeaths(db::DuckDB.DB)
    DBInterface.execute(db, create_seq("seq_death_id"))
    sql = raw"""
    CREATE TABLE "deaths" (
    "death_id" INTEGER NOT NULL PRIMARY KEY DEFAULT NEXTVAL('seq_death_id'),
    "site_id" INTEGER NOT NULL REFERENCES "sites" ("site_id"),
    "external_id" TEXT NOT NULL,
    "data_ingestion_id" INTEGER NOT NULL REFERENCES "data_ingestions" ("data_ingestion_id"),
    UNIQUE ("site_id", "external_id")
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "death_rows" (
    "death_id" INTEGER NOT NULL REFERENCES "deaths" ("death_id"),
    "row_id" INTEGER NOT NULL REFERENCES "datarows" ("row_id"),
    PRIMARY KEY ("death_id", "row_id"),
    UNIQUE ("death_id", "row_id")
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end