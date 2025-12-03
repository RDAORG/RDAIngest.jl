using Test
using RDAIngest
using UUIDs
using SQLite

# using Pkg
# using ConfigEnv
# using Logging
# using DBInterface
# using DataFrames
# using Dates
# using CSV
# 
# using XLSX
# using Base64
# using FileIO


@testset "RDAIngest tests" begin
    temp_db = tempname() * ".sqlite"  # generate a unique temporary file path

    try
        # Get environment variables
        dotenv()

        # Create database
        #@time createdatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], replace=true)
        @time createdatabase(ENV["RDA_SERVER"], ENV["RDA_DBNAME"], replace=true, sqlite = false)

        @test isfile(temp_db)

        # Read database
        db = SQLite.DB(temp_db)

        db = opendatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"]; sqlite=true)
        tables = SQLite.tables(db)
        @test length(tables) > 0  # or check expected table names

        
        # 
        
        
        
        # 
        SQLite.close(db)



        

        

        # Query the BLOB
        instru_id=1
        sql = "SELECT document FROM instrument_documents WHERE instrument_id = ?;"
        blob = DataFrame(DBInterface.execute(db, sql, (instru_id,)))[!,:document][1]
        blob = Base64.base64decode(String(blob))

        # Save blob to pdf
        instru_file = "test_doc.pdf"
        open(instru_file, "w") do io
            write(io, blob)
        end
        @test isfile(instru_file)


    finally
        rm(temp_db, force=true)  # clean up
        rm(instru_file, force=true)  # clean up
    end
end