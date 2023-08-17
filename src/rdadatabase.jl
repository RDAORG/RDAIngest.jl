"""
    createdatabase(path, name)

Creates a SQLite database to store the information contained in the Reference Death Archive (RDA)
"""
function createdatabase(path, name; replace=false)
    file = joinpath(path, "$name.sqlite")
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
    db = SQLite.DB(file)
    createsources(db)
    createprotocols(db)
    createtransformations(db)
    createvariables(db)
    createdatasets(db)
    createinstruments(db)
    createdeaths(db)
    createmapping(db) 
    close(db)
end
"""
    opendatabase(path::String, name::String)::SQLite.DB

Open file on path as an SQLite database (assume .sqlite extension)
"""
function opendatabase(path::String, name::String)::SQLite.DB
    file = joinpath(path, "$name.sqlite")
    if isfile(file)
        return SQLite.DB(file)
    else
        error("File '$file' not found.")
    end
end

"""
    get_table(db::SQLite.DB, table::String)::AbstractDataFrame

Retrieve table `table` as a DataFrame from `db`
"""
function get_table(db::SQLite.DB, table::String)::AbstractDataFrame
    sql = "SELECT * FROM $(table)"
    df = DBInterface.execute(db, sql; iterate_rows=true) |> DataFrame
    return df
end
"""
    createsources(db::SQLite.DB)

Creates tables to record a source and associated site/s for deaths contributed to the RDA
"""
function createsources(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "sources" (
    "source_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "sites" (
    "site_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "site_name" TEXT NOT NULL,
    "country_iso2" TEXT NOT NULL,
    "source_id" INTEGER NOT NULL,
    CONSTRAINT "fk_sites_source_id" FOREIGN KEY ("source_id") REFERENCES "sources" ("source_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_source_name"
    ON "sites" (
    "source_id" ASC,
    "site_name" ASC
    );
    """
    DBInterface.execute(db, sql)
end
"""
    createprotocols(db::SQLite.DB)

Create tables to record information about protocols and the ethics approvals for those protocols
"""
function createprotocols(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "ethics" (
    "ethics_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "ethics_committee" TEXT NOT NULL,
    "ethics_reference" TEXT NOT NULL,
    CONSTRAINT "fk_ethics_ethics_id" FOREIGN KEY ("ethics_id") REFERENCES "sources" ("source_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "ethics_documents" (
    "ethics_document_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "ethics_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT NOT NULL,
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
    "description" TEXT NOT NULL,
    "ethics_id" INTEGER,
    CONSTRAINT "fk_protocols_ethics_id" FOREIGN KEY ("ethics_id") REFERENCES "ethics" ("ethics_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_site_name"
    ON "protocols" (
    "site_id" ASC,
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
end
"""
    createvariables(db)

Create tables to record value types, variables and vocabularies
"""
function createvariables(db)
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
    "name" TEXT NOT NULL,
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
    "key" TEXT,
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
end
"""
    createdatasets(db::SQLite.DB)

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

end
"""
    createinstruments(db::SQLite.DB)

Create tables to record data collection instruments, and their associated protocols and datasets
"""
function createinstruments(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "instruments" (
    "instrument_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    "description" TEXT NOT NULL
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
    "intrument_document_id" INTEGER NOT NULL,
    "instrument_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "document" BLOB,
    PRIMARY KEY ("intrument_document_id"),
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
end
"""
    createdeaths(db)

Create tables to store deaths, and their association with data rows and data ingests
"""
function createdeaths(db)
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
end


"""
    createmapping(db)

Create the table required for variable mapping. This table is used to map variables from one instrument to another. The table is created in the database provided as an argument.
The variable mapping is based on the PyCrossVA approach.

The relationship to the PyCrossVA configuration file columns:

  * New Column Name = destination_id - the variable_id of the new column
  * New Column Documentation = Stored in the variable table
  * Source Column ID = from_id - the variable_id of the source variable
  * Source Column Documentation = will be in the variables table
  * Relationship = operator - the operator to be used to create the new variable
  * Condition = operants - the operants to be used with the operator
  * Prerequisite = prerequisite_id - the variable_id of the prerequisite variable

"""
function createmapping(db)
    sql = raw"""
        CREATE TABLE IF NOT EXISTS "variablemaps" (
            "variablemap_id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            "name"	TEXT NOT NULL,
            "instrument_id"	INTEGER NULL,
            "source_domain" TEXT NULL,
            "destination_domain" TEXT NULL,
            CONSTRAINT "fk_variablemaps_instruments" FOREIGN KEY("instrument_id") REFERENCES "instruments"("instrument_id")
        );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS variablemappings (
        variablemapping_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        variablemap_id INTEGER NOT NULL,
        destination_id INTEGER NOT NULL,
        from_id INTEGER NULL,
        operator TEXT NULL,
        operants TEXT NULL,
        prerequisite_id INTEGER NULL,
        CONSTRAINT fk_variablemappings_variablemap_id FOREIGN KEY (variablemap_id) REFERENCES variablemaps (variablemap_id),
        CONSTRAINT fk_variablemappings_destination_id FOREIGN KEY (destination_id) REFERENCES variables (variable_id),
        CONSTRAINT fk_variablemappings_from_id FOREIGN KEY (from_id) REFERENCES variables (variable_id),
        CONSTRAINT fk_variablemappings_prerequisite_id FOREIGN KEY (prerequisite_id) REFERENCES variables (variable_id)
    );
    """
    DBInterface.execute(db, sql)

end
