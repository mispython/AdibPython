import polars as pl

input_file = "input.parquet"
output_file = "output.csv"

df = pl.read_parquet(input_file)
df.write_csv(output_file)

print("Conversion completed!")
