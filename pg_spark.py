from pyspark.sql import SparkSession

appName  = "PostgresPySpark"
# Download the PostgreSQL JDBC driver from https://jdbc.postgresql.org/download.html
jar_path = "" #/path/to/postgresql-xxxx.jar
# Create SparkSession
spark = SparkSession.builder \
    .appName(appName) \
    .config("spark.jars", jar_path) \
    .getOrCreate()

# Update PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://POSTGRES_HOST:5432/POSTGRES_DB"
connection_properties = {
    "user": "USERNAME",
    "password": "PASSWORD",
    "driver": "org.postgresql.Driver"
}

# Read entire table data from postgres into a DataFrame
table_data_df = spark.read.jdbc(url=jdbc_url, table="your_table", properties=connection_properties)
df.show()

# Read table data with a specific column from postgres into a DataFrame
table_data_df = spark.read.jdbc(url=jdbc_url, table="your_table", column="column_name", properties=connection_properties)


# Read a data from a query into a DataFrame
query = "(SELECT * FROM your_table WHERE column_name = 'value') AS query_table"
query_data_df = spark.read.jdbc(url=jdbc_url, query=query, properties=connection_properties)

# More options here https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html


# Stop SparkSession
spark.stop()
