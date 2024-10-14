import time
from py2neo import Graph

# Connect to the Neo4j database
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

# Test the connection
try:
    graph.run("RETURN 1")
    print("Connected to Neo4j!")
except Exception as e:
    print("Failed to connect to Neo4j:", e)
    
csv_files = ["file:///0.csv","file:///1.csv","file:///2.csv","file:///3.csv"]
csv_files_for_testing = ["file:///0.csv","file:///1.csv"]
def create_nodes_from_csv(csv_file):
  query_create_nodes = """
  LOAD CSV WITH HEADERS FROM $csv_file AS row
  MERGE (v:Video {video_id: row.`video ID`})
  SET v.uploader = row.uploader,
      v.age = toInteger(row.age),
      v.category = row.category,
      v.length = toInteger(row.length),
      v.views = toInteger(row.views),
      v.rate = toFloat(row.rate),
      v.ratings = toInteger(row.ratings),
      v.comments = toInteger(row.comments),
      v.related_ids = row.`related IDs`
  """

  start_time = time.time()  # Start timer
  graph.run(query_create_nodes, csv_file=csv_file)
  end_time = time.time()  # End timer
  print(f"Nodes created successfully from {csv_file} in {end_time - start_time:.2f} seconds.")

# Query to create relationships between videos and their related IDs
def create_relationships_from_csv(csv_file):
  query_create_relationships = """
  LOAD CSV WITH HEADERS FROM $csv_file AS row
  MATCH (v:Video {video_id: row.`video ID`})
  WITH v, SPLIT(row.`related IDs`, ",") AS related
  UNWIND related AS related_id
  MATCH (r:Video {video_id: related_id})
  MERGE (v)-[:RELATED]->(r)
  """

  # Execute the query to create relationships
  graph.run(query_create_relationships, csv_file=csv_file)
  print("Relationships created successfully.")
  
# Function to create relationships between existing nodes in the database
def create_relationships_between_nodes():
    query_create_relationships = """
    MATCH (v:Video)
    WITH v, SPLIT(v.related_ids, ",") AS related
    UNWIND related AS related_id
    MATCH (r:Video {video_id: related_id})
    MERGE (v)-[:RELATED]->(r)
    """
    start_time = time.time()  # Start timer
    graph.run(query_create_relationships)
    end_time = time.time()  # End timer
    print(f"Relationships created successfully between existing nodes in {end_time - start_time:.2f} seconds.")
    
# Loop through each CSV file in the list and load data into Neo4j
print("Starting node creation...")
total_node_creation_time = 0
for csv_file in csv_files_for_testing:
    # First, create the nodes for the video data
    start_time = time.time()  # Start timer for node creation per file
    create_nodes_from_csv(csv_file)
    total_node_creation_time += (time.time() - start_time)
    
    # Then, create relationships between the videos and their related videos
    #create_relationships_from_csv(csv_file)

print(f"Total time for node creation from all CSV files: {total_node_creation_time:.2f} seconds.")

print("Creating relationships between existing nodes...")
start_time = time.time()  # Start timer for relationship creation
create_relationships_between_nodes()
total_relationship_creation_time = time.time() - start_time
print(f"Total time for relationship creation: {total_relationship_creation_time:.2f} seconds.")

total_time = total_node_creation_time + total_relationship_creation_time
print(f"Total time for node and relationship creation: {total_time:.2f} seconds.")