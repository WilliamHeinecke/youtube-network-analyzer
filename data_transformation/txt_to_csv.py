import pandas as pd

# Define the column names
columns = ['video ID', 'uploader', 'age', 'category', 'length', 'views', 'rate', 'ratings', 'comments', 'related IDs']

# Read the file and split each line by the tab delimiter
with open('../data/0222/4.txt', 'r') as file:
    lines = file.readlines()

# Split each line and organize the data
data = []
for line in lines:
    values = line.strip().split('\t')
    related_ids = ','.join(values[9:])  # Combine the related IDs into a single string
    row = values[:9] + [related_ids]  # Add the related IDs as the last column
    data.append(row)

# Create a pandas DataFrame
df = pd.DataFrame(data, columns=columns)

# Save the DataFrame to a CSV file
df.to_csv('../data/0222_csv/4.csv', index=False)
