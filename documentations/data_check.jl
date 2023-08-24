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
freqtable(champs_raw, :"year_Id10021")

# Missing patterns by sites (site_iso_code)
freqtable(champs_raw, :"year_Id10021" , :"site_iso_code")

# Demo data 

# Create the "year_of_birth" variable (including missing) to check year distribution
champs_raw.year_of_birth = [ismissing(date) ? missing : year(date) for date in champs_raw.date_of_birth]
freqtable(champs_raw, :"year_of_birth")

# Missing patterns by sites (site_iso_code)
freqtable(champs_raw, :"year_of_birth" , :"site_iso_code")

#### c. Date of Death
# VA data 

# Create the "year_of_death" variable (including missing) to check year distribution
champs_raw.year_Id10023 = [ismissing(date) ? missing : year(date) for date in champs_raw.Id10023]
freqtable(champs_raw, :"year_Id10023")

# Missing patterns by sites (site_iso_code)
freqtable(champs_raw, :"year_Id10023" , :"site_iso_code")

# Demo data 

# Create the "year_of_death" variable (including missing) to check year distribution
champs_raw.year_of_death = [ismissing(date) ? missing : year(date) for date in champs_raw.date_of_death]
freqtable(champs_raw, :"year_of_death")

# Missing patterns by sites (site_iso_code)
freqtable(champs_raw, :"year_of_death" , :"site_iso_code")

#### d. Age days/months/years
describe(champs_raw[!, :ageInDays])
describe(champs_raw[!, :ageInMonths])
describe(champs_raw[!, :ageInYears2])
describe(champs_raw[!, :ageInYearsRemain])

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


### 1) Missing Variables 
#### The list of variables only of missing value is here 
println("Variables with type Missing:")
for col_name in names(champs_raw)
    if eltype(champs_raw[!, col_name]) == Missing
        println(col_name)
    end
end

### 2) Mistmatched data types 
#### a. Id10106:
freqtable(champs_raw, :"Id10106")



###########################################
### 2) Categorical Variables (String)
#### eg 1. At any time during the final illness was there blood in the stools?
freqtable(champs_raw, :"Id10186")

#### eg 2. What was her/his citizenship/nationality?
freqtable(champs_raw, :"Id10052")


### 3) Numerical Variables (Integer/Float)

##### Calculated ages have so many missing values. 
describe(champs_raw[!, :ageInDays])
describe(champs_raw[!, :ageInYears])
describe(champs_raw[!, :ageInYears2])
describe(champs_raw[!, :ageInYearsRemain])


### 4) Date Variables
### Validity issues with all the date variables 
date_union_vars = []
date_vars = []
for col_name in names(champs_raw)
    col_type = eltype(champs_raw[!, col_name])
    if col_type == Union{Missing, Date}
        push!(date_union_vars, col_name)
    elseif col_type == Date
        push!(date_vars, col_name)
    end
end

println("Variables of dates:")
for var in append!(date_union_vars, date_vars)
    println(var)
end

### Id10012: Date of interview
# Extract the "Id10012" date column
id10012_dates = champs_raw[!, :Id10012]
# Count missing values
id12_missing = count(ismissing, id10012_dates)
# Remove missing values
id12_dates = filter(x -> !ismissing(x), id10012_dates)

# Calculate the minimum and maximum valid dates
if !isempty(id12_dates)
    min_date = minimum(id12_dates)
    max_date = maximum(id12_dates)
    println("Range of valid dates for Id10012: $min_date to $max_date")
    println("Number of missing values: $id12_missing")
else
    println("No valid dates found.")
end

### Source data for Age Calculation //Calculated: (${Id10023} - ${Id10021})
## Id10021: When was the deceased born?
# Extract the "Id10021" date column
id10021_dates = champs_raw[!, :Id10021]
# Count missing values
id21_missing = count(ismissing, id10021_dates)
# Remove missing values
id21_dates = filter(x -> !ismissing(x), id10021_dates)

# Calculate the minimum and maximum valid dates
if !isempty(id21_dates)
    min_date = minimum(id21_dates)
    max_date = maximum(id21_dates)
    println("Range of valid dates for Id10021: $min_date to $max_date")
    println("Number of missing values: $id21_missing")
else
    println("No valid dates found.")
end

## Id10023: When did (s)he die?
# Extract the "Id10023" date column
id10023_dates = champs_raw[!, :Id10023]
# Count missing values
id23_missing = count(ismissing, id10023_dates)
# Remove missing values
id23_dates = filter(x -> !ismissing(x), id10023_dates)

# Calculate the minimum and maximum valid dates
if !isempty(id23_dates)
    min_date = minimum(id23_dates)
    max_date = maximum(id23_dates)
    println("Range of valid dates for Id10023: $min_date to $max_date")
    println("Number of missing values: $id23_missing")
else
    println("No valid dates found.")
end


### 5) Time variable - format issues 
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

#############################################
### STEP 2. Detect the missing data patterns
#############################################

# By region
# Id10054: Place of Birth // no valid info on the geographic information
freqtable(champs_raw, :"Id10054")

# Just few info on the limited facility info
# Id10058: Where did the deceased die?
freqtable(champs_raw, :"Id10058")

freqtable(champs_raw, :"Id10433")






#############################################
### STEP 3. CLEAN THE DATA (not processed yet)
#############################################

# 1. Every string variable should be in the lowercase.

# Create a copy of the original DataFrame
champs_df3 = copy(champs_df2)

# Check the datatypes for each column
for col_name in names(champs_df3)
    col = champs_df3[!, col_name]
    println("$col_name: $(eltype(col))")
end

# Loop through each column in the copy
for col_name in names(champs_df3)
    col = champs_df3[!, col_name]
    col_type = eltype(col)
    if col_type == Union{Missing, String}
        champs_df3[!, col_name] .= map(x -> x isa Missing ? x : lowercase(x), col)
    end
end

# Save the DataFrame to a CSV file to check the dataframe
CSV.write("champs_df3.csv", champs_df3)

## STEP 2: Make the "DK" mising data consistent
using Statistics
using Pkg
#Pkg.add("FreqTables")
using FreqTables

# Check the frequency of each variable
freqtable(champs_df3, :"Id10186")

# Define a function to replace "doesn't know" and "does not know" with "dk"
replace_dk(text) = ismissing(text) ? missing : (text == "doesn't know" || text == "does not know" ? "dk" : text)

# Apply the replace_dk function to the entire DataFrame
for col_name in names(champs_df3)
    col = champs_df3[!, col_name]
    if eltype(col) <: Union{Missing, String}
        champs_df3[!, col_name] .= replace_dk.(col)
    end
end

# Check again for sanity
freqtable(champs_df3, :"Id10186")

# Save the DataFrame to a CSV file to check the dataframe
CSV.write("champs_df3.csv", champs_df3)


