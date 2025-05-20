import pandas as pd
from pathlib import Path

# Create a set to store unique themes
unique_themes = set()

parquet_path = Path(__file__).parent / "lichess_db_puzzle.parquet"
stream=open(parquet_path, "rb")
# Read the CSV file
df = pd.read_parquet(stream, engine='pyarrow') # pyarrow makes this 10x faster

# Get all unique themes from the themes column
unique_themes = df['openingTags'].unique()

unique_themes_list = []
for themes in unique_themes: 
    themes = themes.split(',')
    for theme in themes:
        if theme not in unique_themes_list:
            unique_themes_list.append(theme)

# Sort the list alphabetically
unique_themes_list.sort()

# Write to file with nice formatting
output_path = Path(__file__).parent / "list_of_openings.txt"
with open(output_path, 'w') as f:
    for i, theme in enumerate(unique_themes_list, 1):
        f.write(f"{theme.replace('_', ' ')}\n")

print(f"Openings list has been written to {output_path}")