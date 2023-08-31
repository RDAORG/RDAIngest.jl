#Test for cleaning and update the VA dataset


# Create a copy of the original DataFrame
champs_df3 = copy(champs_raw)

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


function lowercase_strings!(df::DataFrame)
    for i in 1:size(df, 1)
        for j in 1:size(df, 2)
            if !ismissing(df[i, j]) && isa(df[i, j], String)
                df[i, j] = lowercase(df[i, j])
            end
        end
    end
end


lowercase_strings!(champs_df3)

freqtable(champs_df3,:"Id10002")



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


