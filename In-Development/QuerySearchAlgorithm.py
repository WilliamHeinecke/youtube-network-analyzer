### Programmer: Moises Carranza
### Date: 11/17/2024 (Most current updates)
### Project: Youtube-Network-Analyzer

'''
    Search Engine Algorithm for the Youtube-Network-Analyzer Database stored in a Neo4j Graph database.

    Current Work:
    * Developed basic search algorith struture for the Neo4j search engine
    * Search Methods Completed:
        - Property Based matching

    To-DO:
    * Incorporate PySpark for easier data management, and merging with the database to update the results.
    * Develop all search methods
    * Full Text processing for user queries
    * Incorporate PageRank Algorithm

'''

from py2neo import Graph

class Neo4jSearchEngine:
    def __init__(self, uri, user, password, db):
        self.driver = Graph(uri, name=db, auth=(user, password))

    def CloseConnection(self):
        self.driver.close()

    def search(self, label, property_name, query, match_type="exact", tolerance=3):
        '''
            Search algrothim for Neo4j database that searches nodes based on various types of matching techniques.
            
            :param label - The label of the nodes to search (i.e., Video)
            :param property_name - This is the property element, which is the property to search (i.e., property = Comedy --> search all videos with property "Comedy").
            :param query - This is the search query string.
            :param match_type - This represents the type of search to perform on the provided query. Types of search - Exact, Contains, Starts With, Ends With, Full Text, Levenshtein.
            :param tolerance - Levenshten distance tolerance, which is only used when match_type="levenshtein".
            :return - List of all nodes matching the search criteria. 
        '''

        with self.driver.session() as session:
            if match_type == "exact":
                result = session.read_transaction(self._exact_match, label, property_name, query)
            elif match_type == "contains":
                result = session.read_transaction(self._contains_match, label, property_name, query)
            elif match_type == "starts_with":
                result = session.read_transaction(self._starts_with_match, label, property_name, query)
            elif match_type == "ends_with":
                result = session.read_transaction(self._ends_with_match, label, property_name, query)
            elif match_type == "fulltext":
                result = session.read_transaction(self._fulltext_match, label, property_name, query)
            elif match_type == "levenshtein":
                result = session.read_transaction(self._levenshtein_match, label, property_name, query)
            else:
                raise ValueError(f"Invalid Match Type provided - {match_type}")
            return result

    def get_property_types(self):
        '''
            Retrieve all node labels and their property types from the database.
            :return - Returns a dictionary where the keys are labesl and the values are dictionaries of property types as the key and their type as the value.
        ''' 
        with self.driver.session() as session:
            result = session.read_transaction(self._fetch_property_types)
            return result
        
    def get_property_values(self, label, property_name):
        """
            Function that retrieves all unique values of a specified property for nodes with a given label.

            :param label - The label of the nodes within the database (i.e., Video)
            :param property_name - The property name selected to retrieve its values (i.e., category)
            :return - Returns a list of unique property values
        """

        with self.driver.session() as session:
            result = session.read_transaction(self._fetch_property_values, label, property_name)
            return result
        
    

    @staticmethod
    def _exact_match(tx, label, property_name, query):
        query_text = f'''
        MATCH (n:{label})
        WHERE n.{property_name} = $query
        RETURN n AS node
        '''

        result = tx.run(query_text, query=query)
        return [record["node"] for record in result]

    @staticmethod
    def _contains_match(tx, label, property_name, query):
        query_text = f'''
        MATCH (n:{label}) 
        WHERE n.{property_name} 
        CONTAINS $query 
        RETURN n AS node
        '''

        result = tx.run(query_text, query=query)
        return [record["node"] for record in result]

    @staticmethod
    def _starts_with_match(tx, label, property_name, query):
       query_text = f'''
       MATCH (n:{label}) 
       WHERE n.{property_name} 
       STARTS WITH $query 
       RETURN n AS node
       '''
       
       result = tx.run(query_text, query=query)
       return [record["node"] for record in result]

    @staticmethod
    def _ends_with_match(tx, label, property_name, query):
        query_text = f'''
        MATCH (n:{label}) 
        WHERE n.{property_name} 
        ENDS WITH $query 
        RETURN n AS node
        '''

        result = tx.run(query_text, query=query)
        return [record["node"] for record in result]

    @staticmethod
    def _fulltext_match(tx, label, property_name, query):
        ## Ensure that Full-text indices exists before querying
        index_name = f"{label}Index"
        query_text = f"""
        CALL db.index.fulltext.queryNodes('{index_name}', $query)
        YIELD node, score
        RETURN node, score
        ORDER BY score DESC
        """

        result = tx.run(query_text, query=query)
        return [{"node": record["node"], "score": record["score"]} for record in result]

    @staticmethod
    def _levenshtein_match(tx, label, property_name, query, tolerance):
        ## Requires APOC (Awesome Procedure of Cyphers) Library for Levenshtein distance algorithm
        query_text = f"""
        MATCH  (n:{label})
        WHERE apoc.text.levenshteinDistance(n.{property_name}, $query) <= $tolerance
        RETURN n AS node
        """

        result = tx.run(query_text, query=query, tolerance=tolerance)
        return [record["node"] for record in result]
    
    @staticmethod
    def _fetch_property_types(tx):
        """
            Transactional function to identify property types for each node label.    
        """

        query = """
        CALL db.schema.nodeTypeProperties() YIELD nodeType, propertyName, propertyTypes
        RETURN nodeType AS label, propertyName, propertyTypes
        """

        result = tx.run(query)
        schema = {}

        for record in result:
            label = record["label"][0] ## Labels should be returned as lists
            property_name = record["propertyName"]
            property_types = record["propertyTypes"]

            if label not in schema:
                schema[label] = {}
            schema[label][property_name] = property_types
        
        return schema
    
    @staticmethod
    def _fetch_property_values(tx, label, property_name):
        """
            Transactional function to fetch all unique values of the selected property.
        """

        query = f"""
        MATCH (n:{label})
        WHERE exists(n:{property_name})
        RETURN DISTINCT n.{property_name} as value
        """

        result = tx.run(query)
        return [record["value"] for record in result]

    
def main(se: Neo4jSearchEngine):
    pass

if __name__ == "__main__":
    # Connect to the Neo4j database
    uri = "bolt://localhost:7687"
    dbName = "youtube-network-analyzer"
    user = "neo4j"
    password = "youtubeDBMS"
    searchEngine = Neo4jSearchEngine(uri=uri, db=dbName, user=user, password=password)
    main(se=searchEngine)