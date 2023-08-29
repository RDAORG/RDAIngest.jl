# Data quality check 
# CHAMPS VA + Demographics datasets
# Young 
# Last Updated: Aug 24 2023

using ConfigEnv
using DBInterface
using DataFrames
using Dates
using BenchmarkTools
using SQLite
using RDAIngest #just run the codes in RDAIngest.jl / rdadatabase.jl 
using Dates
using Arrow
using CSV
using GR
Pkg.add("PyPlot")
using PyPlot          
using Plots


#Get environment variables #change the path in .env file
dotenv()

#Get the DB in SQL outputdatasets 
db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")

# Example codes to get the dataset to dataframe
DBInterface.execute(db, "SELECT * FROM value_types";) |> DataFrame
DBInterface.execute(db, "SELECT * FROM datasets";) |> DataFrame

##############################################
### STEP 0. Set up the Merged dataset VA and Demographics
##############################################

# A. Set up the [CHAMPS verbal autopsy] as dataframe -> dataset_id ==5
# Query data for rows with dataset_id = 5
data_query1 = """
SELECT row_id, variable_id, value
FROM data
WHERE row_id IN (SELECT row_id FROM datarows WHERE dataset_id = 5)
"""
champs_data1 = DBInterface.execute(db, data_query1)|>DataFrame

# Query variables for variable information
variables_query1 = """
SELECT variable_id, name, value_type_id
FROM variables
WHERE variable_id IN (SELECT DISTINCT variable_id FROM data WHERE row_id IN (SELECT row_id FROM datarows WHERE dataset_id = 5))
"""
champs_vars1 = DBInterface.execute(db, variables_query1)|> DataFrame

# Merge data_df with variables_df based on variable_id and make it to wide format
champs_va = innerjoin(champs_data1, champs_vars1, on = :variable_id)
champs_va2 = unstack(champs_va, :row_id,  :name, :value)
rename!(champs_va2, :row_id => :row_id_va, :age_group => :age_group_va) #rename overlapped variables


# B. Set up the [CHAMPS degmoraphics] as dataframe -> dataset_id ==3
# Query data for rows with dataset_id = 3
data_query2 = """
SELECT row_id, variable_id, value
FROM data
WHERE row_id IN (SELECT row_id FROM datarows WHERE dataset_id = 3)
"""
champs_data2 = DBInterface.execute(db, data_query2)|>DataFrame

# Query variables for variable information
variables_query2 = """
SELECT variable_id, name, value_type_id
FROM variables
WHERE variable_id IN (SELECT DISTINCT variable_id FROM data WHERE row_id IN (SELECT row_id FROM datarows WHERE dataset_id = 3))
"""
champs_vars2 = DBInterface.execute(db, variables_query2)|> DataFrame

# Merge data_df with variables_df based on variable_id and make it to wide format
champs_demo = innerjoin(champs_data2, champs_vars2, on = :variable_id)
champs_demo2 = unstack(champs_demo, :row_id,  :name, :value)
rename!(champs_demo2, :row_id => :row_id_demo, :age_group => :age_group_demo) #rename overlapped variables

# A+B Merging two datasets
# Merge data VA and Demographics
champs_vd=outerjoin(champs_va2, champs_demo2, on = :champs_deid)



# Wide format based on unique row ID and variable name

names(champs_vd) # 492 variables 

# Save the DataFrame to a CSV file to explore the dataframe
CSV.write("champs_vd.csv", champs_vd)

# Check the unique values of each variable ()
unique(champs_vd[!, "Id10359"])

"""# Problematic variables from 457 variables
fix type: ["Id10024", "Id10248","Id10250","Id10262","Id10266","Id10352"] 
Surely need to clean values: ["Id10106", "Id10108", "Id10161_0","Id10161_1","Id10162","Id10202","Id10221",
               "Id10285","Id10358","Id10359","Id10367","Id10379","Id10380","Id10382","Id10392","Id10394"
               ] 
"""
#######################################
### STEP 1. Documentation of data quality of raw data in Markdown 
###(CHAMPS_VA_Quality_Check.jmd)
#######################################

## 0. Set up the CHAMPS VA dataset
using CSV
using DataFrames
using Pkg
using Dates
"FreqTables" ∉ keys(Pkg.project().dependencies) && Pkg.add("FreqTables")
"StatsBase" ∉ keys(Pkg.project().dependencies) && Pkg.add("StatsBase")
"PyPlot" ∉ keys(Pkg.project().dependencies) && Pkg.add("PyPlot")
"Plots" ∉ keys(Pkg.project().dependencies) && Pkg.add("Plots")
using PyPlot          
using Plots
using FreqTables
using StatsBase
filename = "champs_vd"
path = "/Users/young/Documents/GitHub/RDAIngest.jl/"
file = joinpath(path, "$filename.csv")
champs_raw = CSV.File(file; delim=',', quotechar='"', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame


## 1. Dataset Overview

### 1) Data Size
size(champs_raw)

### 2) Data types 
column_types = Dict{Symbol, Type}()
for col in names(champs_raw)
    column_types[Symbol(col)] = eltype(champs_raw[!, col])
end

unique_types = Set(values(column_types))

type_counts = Dict{Type, Int}()
for col_type in values(column_types)
    type_counts[col_type] = get(type_counts, col_type, 0) + 1
end

println("Summary of variable types:")
for (col_type, count) in type_counts
    println("$col_type: $count variables")
end



## 2. Exploring Key Variables
### 1) Cross-check the key variables between VA and demographics 
#### a. Age group 
# VA data
freqtable(champs_raw, :"age_group_va")
# Demographics data
freqtable(champs_raw, :"age_group_demo")

# age group in VA data does not seem complete / demographics seems ok 

#### b. Date of Birth
# VA data 

# Create the "year_of_birth" variable (including missing) to check year distribution
champs_raw.year_Id10021 = [ismissing(date) ? missing : year(date) for date in champs_raw.Id10021]
# Distribution of year of birth
freqtable(champs_raw, :"year_Id10021")

# Missing patterns by sites (site_iso_code)
freqtable(champs_raw, :"year_Id10021" , :"site_iso_code")

# Demo data 

# Create the "year_of_birth" variable (including missing) to check year distribution
champs_raw.year_of_birth = [ismissing(date) ? missing : year(date) for date in champs_raw.date_of_birth]
# Distribution of year of birth 
freqtable(champs_raw, :"year_of_birth")

# Missing patterns by sites (site_iso_code)
freqtable(champs_raw, :"year_of_birth" , :"site_iso_code")

#### c. Date of Death
# VA data 

# Create the "year_of_death" variable (including missing) to check year distribution
champs_raw.year_Id10023 = [ismissing(date) ? missing : year(date) for date in champs_raw.Id10023]
# Distribution of year of death
freqtable(champs_raw, :"year_Id10023")

# Missing patterns by sites (site_iso_code)
freqtable(champs_raw, :"year_Id10023" , :"site_iso_code")

# Demo data 

# Create the "year_of_death" variable (including missing) to check year distribution
champs_raw.year_of_death = [ismissing(date) ? missing : year(date) for date in champs_raw.date_of_death]
# Distribution of year of death
freqtable(champs_raw, :"year_of_death")

# Missing patterns by sites (site_iso_code)
freqtable(champs_raw, :"year_of_death" , :"site_iso_code")

#### d. Age days/months/years
# VA data
describe(champs_raw[!, :ageInDays])
describe(champs_raw[!, :ageInMonths])
describe(champs_raw[!, :ageInYears2])
describe(champs_raw[!, :ageInYearsRemain])

# Demo data
describe(champs_raw[!, :age_days])
describe(champs_raw[!, :age_months])
describe(champs_raw[!, :age_years])

#### e. Sanity check for calculation of age in days

# VA data
champs_raw.ageindays_va_check = [ismissing(id21) || ismissing(id23) ? missing : Int(Dates.value(id23) - Dates.value(id21)) for (id21, id23) in zip(champs_raw.Id10021, champs_raw.Id10023)]
describe(champs_raw[!, :ageindays_va_check]) # does't make sense 

# Demo data
champs_raw.ageindays_dm_check = [ismissing(id21) || ismissing(id23) ? missing : Int(Dates.value(id23) - Dates.value(id21)) for (id21, id23) in zip(champs_raw.date_of_birth, champs_raw.date_of_death)]
describe(champs_raw[!, :ageindays_dm_check]) # hmm




### 2) Mistmatched data types 
# VA data
#### - Id10106: How many minutes after birth did the baby first cry?
freqtable(champs_raw, :"Id10106")
#### - Id10108: How many hours before death did the baby stop crying?
freqtable(champs_raw, :"Id10108")

#### - Id10024: Please indicate the year of death.
freqtable(champs_raw, :"Id10024") 
# Create the "year_of_death" variable (including missing) to check year distribution
champs_raw.year_Id10024 = [ismissing(date) ? missing : year(date) for date in champs_raw.Id10024]
# Distribution of year of death
freqtable(champs_raw, :"year_Id10024")
# so many missings on this and it is supposed to be year 

#### - Id10162: For how many months did the difficulty breathing last?
freqtable(champs_raw, :"Id10162")
# How to assign months in numbers?

#### - Id10248: For how many days did (s)he have puffiness of the face?
freqtable(champs_raw, :"Id10248")

#### - Id10250: How many days did the swelling last? Calculated as: if(${Id10250_units}='days', ${Id10250_a} div 30,${Id10250_b})
freqtable(champs_raw, :"Id10250")



### 3) Invalid measures or answers 
# VA data
#### -Id10059: What was her/his marital status?
freqtable(champs_raw, :"Id10059")

#### -Id10213: For how many months did (s)he have mental confusion? Calculated as: if(${Id10213_units}='days', ${Id10213_a} div 30,${Id10213_b})
freqtable(champs_raw, :"Id10213")

#### -Id10178: How many minutes did the chest pain last?
freqtable(champs_raw, :"Id10178")




### 4) Inconsistent coding in answers 
#### for most of the categorical variables - 

#### - Id10004: Did s(he) die during the wet season? 
freqtable(champs_raw, :"Id10004")
# Need to extract the first word

#### - Id10052: What was her/his citizenship/nationality?
freqtable(champs_raw, :"Id10052")
# Standardized underbar between more than one words

#### - Id10077:  Did (s)he suffer from any injury or accident that led to her/his death? 
freqtable(champs_raw, :"Id10077")
# Many Y/N answers look like this 

## List of Y/N variables 
# Function to get column names with "yes" or "no" values
function filter_yes_no_columns(df::DataFrame)
    selected_cols = String[]
    for col in names(df)
        col_values = df[!, col]
        has_yes_no_values = false
        for val in col_values
            if !ismissing(val) && (val == "yes" || val == "no")
                has_yes_no_values = true
                break
            end
        end
        if has_yes_no_values
            push!(selected_cols, col)
        end
    end
    return selected_cols
end

# Get column names with "yes" or "no" values
yes_no_columns = filter_yes_no_columns(champs_raw)

println(yes_no_columns)
## 221 variables 


### 5) Uncertain missing value ranges

#### - Id10148: How many days did the fever last?
describe(champs_raw[!,:Id10148])
freqtable(champs_raw, :"Id10148")

describe(champs_raw[!,:Id10154])


freqtable(champs_raw, :"Id10174")



### 6) Formatting issues with time variables
# Extract the "Id10011" column
id10011_column = champs_raw[!, :Id10011]
# Count the number of missing values
id11_missing = count(ismissing, id10011_column)

# Get non-missing values
non_missing_values = id10011_column[.!ismissing.(id10011_column)]
# Create a dictionary to store examples by length
examples_by_length = Dict{Int, String}()

# Iterate through non-missing values
for value in non_missing_values
    length_value = length(value)
    if !haskey(examples_by_length, length_value)
        examples_by_length[length_value] = value
    end
end

# Print example values for different lengths
println("Example values from Id10011:")
for (length, example) in examples_by_length
    println("Length $length: $example")
end


