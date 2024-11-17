from neo4j import GraphDatabase

# Neo4j connection details
URI = "bolt://localhost:7687"  
USERNAME = "neo4j"             
PASSWORD = "password"     

# Establish connection
driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def run_pagerank():
    with driver.session() as session:
        session.run("CALL gds.graph.drop('videoGraph', false) YIELD graphName")
        
        # Create a projected graph using the existing 'RELATED' relationships
        session.run("""
        CALL gds.graph.project(
            'videoGraph',
            'Video',         // Node label
            'RELATED'        // Relationship type
        )
        """)

        # Run the PageRank algorithm
        result = session.run("""
        CALL gds.pageRank.stream('videoGraph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).video_id AS video_id, score
        ORDER BY score DESC
        """)

        # Print results
        print("PageRank Results:")
        for record in result:
            print(f"Video: {record['video_id']}, PageRank Score: {record['score']}")
            
def run_pagerank_and_write():
    with driver.session() as session:
        # Run PageRank and write the results back to the database
        session.run("""
        CALL gds.pageRank.write('videoGraph', {
            writeProperty: 'pageRank'
        })
        YIELD nodePropertiesWritten
        """)
        print("PageRank scores written to 'pageRank' property on Video nodes.")

def get_top_10_videos_by_page_rank():
  with driver.session() as session:
    result = session.run("""
          CALL gds.pageRank.stream('videoGraph')
          YIELD nodeId, score
          RETURN gds.util.asNode(nodeId).video_id AS video_id, score
          ORDER BY score DESC
          """)
    print("PageRank Results:")
    # Store the results in a list
    results_list = [record for record in result]

    # Iterate over the stored results
    for i, record in enumerate(results_list[:10], start=1):  # Limit to the top 10 records
        print(f"{i}. Video: {record['video_id']}, PageRank Score: {record['score']}")

# Run the script
if __name__ == "__main__":
    run_pagerank()
    run_pagerank_and_write()
    get_top_10_videos_by_page_rank()
    driver.close()
