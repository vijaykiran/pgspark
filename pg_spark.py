from pyspark.sql import SparkSession
class DBConfig:
    def __init__(self, host, port, database, username, password):
        self.connection_properties = {
            "user": username,
            "password": password,
            "driver": "org.postgresql.Driver"
        }
        self.jdbc_url =     jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"


class PgSparkUtil:

    def __init__(self, appName, jar_path, db_config: DBConfig):
        self.db_config = db_config
        self.spark = SparkSession.builder \
            .appName(appName) \
            .config("spark.jars", jar_path) \
            .getOrCreate()

    def read_table_data(self , table_name):
        table_data_df = self.spark.read.jdbc( table=table_name,
                                             url=self.db_config.jdbc_url,
                                              properties=self.db_config.connection_properties)
        return table_data_df
    
    def read_query_data(self, query):
        query_data_df = self.spark.read.jdbc( url=self.db_config.jdbc_url,
                                              table=query,
                                              properties=self.db_config.connection_properties)
        return query_data_df

if __name__ == "__main__":
    appName  = "PostgresPySpark"
    jar_path = "postgresql-42.7.3.jar"  #Replace this with the path of the postgresql jar file
    # Replace DBConfig parameters using your database details
    db_config = DBConfig(host="localhost", port="5432", username="world", password="world123", database="world-db") 
    pg_spark = PgSparkUtil(appName, jar_path, db_config)

    # Read data from a table into a dataframe
    table_name = "country"
    country_df = pg_spark.read_table_data(table_name)
    # You can use any dataframe methods to process the data
    country_df.show()

    # Here is how you can read data from a query into a dataframe
    query = "(select * from country where continent='Asia') as country"
    asia_country_df = pg_spark.read_query_data(query)
    asia_country_df.show()



