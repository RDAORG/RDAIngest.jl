using DataFrames

# Create a DataFrame
df = DataFrame(A = 1:10, B = 1)

# Convert DataFrame to Matrix
m = Matrix(df)
