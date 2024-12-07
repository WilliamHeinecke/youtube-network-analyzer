from neo4j import GraphDatabase
import time

# source for page rank information and implementation: https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/

URI = "bolt://localhost:7687"  
USERNAME = "neo4j"             
PASSWORD = "password"     

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def run_pagerank():
    with driver.session() as session:
        session.run("CALL gds.graph.drop('videoGraph', false) YIELD graphName")
        
        # Create a projected graph using 'RELATED' relationships
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
    results_list = [record for record in result]

    for i, record in enumerate(results_list[:10], start=1):  
        print(f"{i}. Video: {record['video_id']}, PageRank Score: {record['score']}")

if __name__ == "__main__":
    
    start_time = time.time() 
    run_pagerank()
    run_pagerank_and_write()
    get_top_10_videos_by_page_rank()
    total_page_rank_time = time.time() - start_time
    print(f"Total time for page rank operations: {total_page_rank_time:.2f} seconds.")

    driver.close()
