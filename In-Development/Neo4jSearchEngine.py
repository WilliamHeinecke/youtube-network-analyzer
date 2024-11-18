### Programmer: Moises Carranza
### Date: 11/17/2024 (Most current updates)
### Project: Youtube-Network-Analyzer

'''
    Search Engine Algorithm for the Youtube-Network-Analyzer Database stored in a Neo4j Graph database.

    Current Work:
    * Developed basic search algorithm structure for the Neo4j search engine
    * Search Methods Completed:
        - Property-based matching

    To-Do:
    * Incorporate PySpark for easier data management, and merging with the database to update results.
    * Develop all search methods
    * Full Text processing for user queries
    * Incorporate PageRank Algorithm
'''

from py2neo import Graph

class Neo4jSearchEngine:
    def __init__(self, uri, user, password, db):
        # Establish connection to the Neo4j database
        self.driver = Graph(uri, name=db, auth=(user, password))

    def close_connection(self):
        # Close the Neo4j connection
        self.driver.close()

    def search(self, label, property_name, query, match_type="exact", tolerance=3):
        '''
        Search algorithm for Neo4j database that searches nodes based on various types of matching techniques.

        :param label: Node label to search (e.g., Video)
        :param property_name: Property to search (e.g., category, title)
        :param query: The search query string.
        :param match_type: Type of match (exact, contains, starts_with, ends_with, fulltext, levenshtein)
        :param tolerance: Levenshtein distance tolerance (used when match_type="levenshtein")
        :return: List of nodes matching the search criteria.
        '''
        match_functions = {
            "exact": self._exact_match,
            "contains": self._contains_match,
            "starts_with": self._starts_with_match,
            "ends_with": self._ends_with_match,
            "fulltext": self._fulltext_match,
            "levenshtein": self._levenshtein_match
        }

        if match_type not in match_functions:
            raise ValueError(f"Invalid match type provided: {match_type}")

        result = match_functions[match_type](self.driver, label, property_name, query, tolerance)
        return result

    def get_property_types(self):
        '''
        Retrieve all node labels and their property types from the database.
        
        :return: Dictionary where the keys are labels and values are dictionaries of property types.
        '''

        result = self._fetch_property_types(self.driver)
        return result
        
    def get_property_values(self, label, property_name):
        """
        Retrieve all unique values of a specified property for nodes with a given label.

        :param label: Node label (e.g., Video)
        :param property_name: Property name to retrieve (e.g., category)
        :return: List of unique property values
        """

        result = self._fetch_property_values(self.driver, label, property_name)
        return result
    
    def category_count(self, label, property_name):
        """
            Retreives the number of videos for the selected category.
        """
        query = f"""
        MATCH (n:{label})
        WHERE n.{property_name} IS NOT NULL
        RETURN n.{property_name} AS category, COUNT(n) AS video_count
        ORDER BY video_count DESC
        """
        result = self.driver.run(query)
        return [{"category": record["category"], "video_count":record["video_count"]} for record in result]

    def category_top_videos(self, label, property_name):
        """ 
            Function that retrieves the top ten videos of each category stored in the database.

            :return - Returns a dictionary where the keys are categories and the values are lists containing each categories top ten videos

        """

        query = f"""
        MATCH (n:{label})
        WHERE n.{property_name} IS NOT NULL AND n.ratings IS NOT NULL
        WITH n.{property_name} as category, n
        ORDER BY n.ratings DESC
        WITH category, COLLECT(n)[0..10] AS top_videos
        UNWIND top_videos AS video
        RETURN category, video.video_id AS video_id, video.ratings AS ratings
        ORDER BY category, ratings DESC
        """

        result = self.driver.run(query)
        ## Organize the data
        categories = {}
        for record in result:
            category = record["category"]
            video_data = {"video_id": record["video_id"], "ratings": record["ratings"]}
            if category not in categories:
                categories[category] = []
            categories[category].append(video_data)
        
        return categories

    @staticmethod
    def _exact_match(graph, label, property_name, query, tolerance=None):
        query_text = f'''
        MATCH (n:{label})
        WHERE n.{property_name} = $query
        RETURN n AS node
        '''
        result = graph.run(query_text, query=query)
        return [record["node"] for record in result]

    @staticmethod
    def _contains_match(graph, label, property_name, query, tolerance=None):
        query_text = f'''
        MATCH (n:{label})
        WHERE n.{property_name} CONTAINS $query
        RETURN n AS node
        '''
        result = graph.run(query_text, query=query)
        return [record["node"] for record in result]

    @staticmethod
    def _starts_with_match(graph, label, property_name, query, tolerance=None):
        query_text = f'''
        MATCH (n:{label})
        WHERE n.{property_name} STARTS WITH $query
        RETURN n AS node
        '''
        result = graph.run(query_text, query=query)
        return [record["node"] for record in result]

    @staticmethod
    def _ends_with_match(graph, label, property_name, query, tolerance=None):
        query_text = f'''
        MATCH (n:{label})
        WHERE n.{property_name} ENDS WITH $query
        RETURN n AS node
        '''
        result = graph.run(query_text, query=query)
        return [record["node"] for record in result]

    @staticmethod
    def _fulltext_match(graph, label, property_name, query, tolerance=None):
        # Ensure that Full-text indices exist before querying
        index_name = f"{label}Index"
        query_text = f"""
        CALL db.index.fulltext.queryNodes('{index_name}', $query)
        YIELD node, score
        RETURN node, score
        ORDER BY score DESC
        """
        result = graph.run(query_text, query=query)
        return [{"node": record["node"], "score": record["score"]} for record in result]

    @staticmethod
    def _levenshtein_match(graph, label, property_name, query, tolerance):
        # Requires APOC (Awesome Procedures of Cypher) for Levenshtein distance algorithm
        query_text = f"""
        MATCH (n:{label})
        WHERE apoc.text.levenshteinDistance(n.{property_name}, $query) <= $tolerance
        RETURN n AS node
        """
        result = graph.run(query_text, query=query, tolerance=tolerance)
        return [record["node"] for record in result]

    @staticmethod
    def _fetch_property_types(graph):
        """
        Transaction function to identify property types for each node label.
        """
        query = """
        CALL db.schema.nodeTypeProperties() YIELD nodeType, propertyName, propertyTypes
        RETURN nodeType AS label, propertyName, propertyTypes
        """
        result = graph.run(query)
        schema = {}

        for record in result:
            label = record["label"][0]  # Labels should be returned as lists
            property_name = record["propertyName"]
            property_types = record["propertyTypes"]

            if label not in schema:
                schema[label] = {}
            schema[label][property_name] = property_types
        
        return schema

    @staticmethod
    def _fetch_property_values(graph, label, property_name):
        """
        Transaction function to fetch all unique values of the selected property.
        """
        query = f"""
        MATCH (n:{label})
        WHERE n.{property_name} IS NOT NULL
        RETURN DISTINCT n.{property_name} AS value
        """
        result = graph.run(query)
        return [record["value"] for record in result]

def main(se: Neo4jSearchEngine):
    # Placeholder for main program logic (if needed in the future)
    
    # Test the Database connection
    try:
        se.driver.run("RETURN 1")
        print("Connected to Neo4j!")
        print("*"*len("Connected to Neo4j!"))

        print("Run the following tests")
        print("*"*len("Run the following tests"))

        print("Obtain all Video Categories:")
        print("*"*len("Obtain all Video Categories:"))

        obtain_video_categories(se=se, label="Video", property="category")
        print("*"*50)

        print("\nListing all Categories and their Count:")
        print("*"*len("Listing all Categories and their Count:"))

        obtain_category_count(se=se, label="Video", property="category")
        print("*"*50)

        print("Top 10 Videos of each Category")
        print("*"*len("Top 10 Videos of each Category"))
        categories_top_ten(se=se, label="Video", property="category")


    except Exception as e:
        print("Failed to connect to Neo4j:", e)

    
def obtain_video_categories(se: Neo4jSearchEngine, label:str, property:str):
    try:
        categories = se.get_property_values(label=label, property_name=property)
        print(f"Obtaining all unique values for the following - Node: {label}, Property: {property}")
        for category in categories:
            print(f"- {category}")

    except Exception as e:
        print(f"Error Occurred: {e}")


def obtain_category_count(se: Neo4jSearchEngine, label:str, property:str):
    try:
        categories = se.category_count(label=label, property_name=property)
        for category in categories:
            print(f"Category: {category['category']} - Video Count: {category["video_count"]}")
    
    except Exception as e:
        print(f"Error Occurred: {e}")
    

def categories_top_ten(se: Neo4jSearchEngine, label: str, property: str):
    try:
        categories = se.category_top_videos(label=label, property_name=property)
        for category, videos in categories.items():
            print(f"Category: {category}")
            print("*"*len(f"Category: {category}"))
            for video in videos:
                print(f"    Video ID: {video['video_id']} - Ratings: {video['ratings']}")
            print()
    
    except Exception as e:
        print(f"Error Occurred: {e}")

if __name__ == "__main__":
    # Neo4j Database connection details
    uri = "bolt://localhost:7687"
    db_name = "youtube-network-analyzer"
    user = "neo4j"
    password = "password"
    
    # Initialize the Neo4j Search Engine
    search_engine = Neo4jSearchEngine(uri=uri, user=user, password=password, db=db_name)
    main(se=search_engine)
