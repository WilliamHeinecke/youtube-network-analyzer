from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Neo4jDegreeDistribution") \
    .config("spark.jars", "neo4j-connector-apache-spark_2.12-5.3.2_for_spark_3.jar") \
    .config("spark.neo4j.bolt.url", "bolt://localhost:7687") \
    .config("spark.neo4j.bolt.user", "neo4j") \
    .config("spark.neo4j.bolt.password", "password") \
    .getOrCreate()

# Load data from Neo4j
neo4j_df = spark.read.format("org.neo4j.spark.DataSource") \
    .option("url", "bolt://localhost:7687") \
    .option("authentication.type", "basic") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "password") \
    .option("query", """
        MATCH (v:Video)
        OPTIONAL MATCH (v)-[:RELATED]->(outNode)  // Outgoing relationships
        OPTIONAL MATCH (inNode)-[:RELATED]->(v)  // Incoming relationships
        RETURN 
            v.video_id AS video_id, 
            COUNT(DISTINCT outNode) AS outgoing_degree,
            COUNT(DISTINCT inNode) AS incoming_degree
    """) \
    .load()

# Show the loaded data
print("Loaded Data from Neo4j:")
neo4j_df.show()

# Calculate total degree (sum of incoming and outgoing)
degree_df = neo4j_df.withColumn("total_degree", col("outgoing_degree") + col("incoming_degree"))

# Show the data with degrees
print("Data with Degrees:")
degree_df.show()

# Calculate average, max, and min degrees
degree_stats = degree_df.select(
    avg(col("outgoing_degree")).alias("avg_outgoing_degree"),
    spark_max(col("outgoing_degree")).alias("max_outgoing_degree"),
    spark_min(col("outgoing_degree")).alias("min_outgoing_degree"),
    avg(col("incoming_degree")).alias("avg_incoming_degree"),
    spark_max(col("incoming_degree")).alias("max_incoming_degree"),
    spark_min(col("incoming_degree")).alias("min_incoming_degree"),
    avg(col("total_degree")).alias("avg_total_degree"),
    spark_max(col("total_degree")).alias("max_total_degree"),
    spark_min(col("total_degree")).alias("min_total_degree")
)

# Show the statistics
print("Degree Statistics:")
degree_stats.show()

# Stop Spark session
spark.stop()
