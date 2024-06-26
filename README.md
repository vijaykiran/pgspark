
# PgSpark - PySpark example for interacting with PostgreSQL Database


## Pre Requisites

For local development of a PySpark program you need to install `pyspark` using 
`pip install pyspark`

PySpark connects to postgres database using JDBC, so to test your programs locally, 
you need to dowlnload the latest JDBC Driver for postgres from https://jdbc.postgresql.org/download/

Now you are ready to start working on a Pyspark program. 

## Pyspark Program
Make sure that you have you have access to the postgres database you are trying to connect to.

Using python commandline (interactive shell), you can follow along using these steps

1. Import the necessary modules:
```python
from pyspark.sql import SparkSession
```

2. Create a SparkSession:
```python
spark = SparkSession.builder \
    .appName("PostgreSQL Example") \
    .config("spark.jars", "/path/to/postgresql-<version>.jar") \
    .getOrCreate()
```
Make sure to replace `/path/to/postgresql-<version>.jar` with the actual path to the PostgreSQL JDBC driver JAR file.

3. Define the connection properties:
```python
properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}
```
Replace `"your_username"` and `"your_password"` with your PostgreSQL credentials.

4. Establish the connection:
```python
url = "jdbc:postgresql://your_host:your_port/your_database"
df = spark.read.jdbc(url=url, table="your_table", properties=properties)
```
Replace `"your_host"`, `"your_port"`, `"your_database"`, and `"your_table"` with the appropriate values for your PostgreSQL setup.

5. Perform operations on the DataFrame:
```python
df.show()
```
You can now perform various operations on the DataFrame, such as displaying the data using `show()`.

6. Close the SparkSession:
```python
spark.stop()
```

## Sample database and pyspark execution results

To try out all steps in your local environment - use a postgresql database in docker container. 

Run a sample PostgreSQL database 
```shell
docker run -d -p 5432:5432 ghusta/postgres-world-db:2.11
```

You can use the following credentials to connect to the database which contains 4 tables (city, country, country_language)

```
database : world-db
user : world
password : world123
```

Run the pg_spark.py using 
```shell 
python pg_spark.py
```
You should see the following output
```shell 
24/06/20 19:07:50 WARN Utils: Your hostname, starfeeld.local resolves to a loopback address: 127.0.0.1; using 192.168.68.62 instead (on interface en0)
24/06/20 19:07:50 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
24/06/20 19:07:50 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
+----+--------------------+-------------+--------------------+------------+----------+----------+---------------+---------+---------+--------------------+--------------------+--------------------+-------+-----+
|code|                name|    continent|              region|surface_area|indep_year|population|life_expectancy|      gnp|  gnp_old|          local_name|     government_form|       head_of_state|capital|code2|
+----+--------------------+-------------+--------------------+------------+----------+----------+---------------+---------+---------+--------------------+--------------------+--------------------+-------+-----+
| AFG|         Afghanistan|         Asia|Southern and Cent...|    652090.0|      1919|  22720000|           45.9|  5976.00|     NULL|Afganistan/Afqane...|     Islamic Emirate|       Mohammad Omar|      1|   AF|
| NLD|         Netherlands|       Europe|      Western Europe|     41526.0|      1581|  15864000|           78.3|371362.00|360478.00|           Nederland|Constitutional Mo...|             Beatrix|      5|   NL|
| ANT|Netherlands Antilles|North America|           Caribbean|       800.0|      NULL|    217000|           74.7|  1941.00|     NULL|Nederlandse Antillen|Nonmetropolitan T...|             Beatrix|     33|   AN|
| ALB|             Albania|       Europe|     Southern Europe|     28748.0|      1912|   3401200|           71.6|  3205.00|  2500.00|           Shqipëria|            Republic|      Rexhep Mejdani|     34|   AL|
| DZA|             Algeria|       Africa|     Northern Africa|   2381741.0|      1962|  31471000|           69.7| 49982.00| 46966.00|  Al-Jaza’ir/Algérie|            Republic|Abdelaziz Bouteflika|     35|   DZ|
| ASM|      American Samoa|      Oceania|           Polynesia|       199.0|      NULL|     68000|           75.1|   334.00|     NULL|       Amerika Samoa|        US Territory|      George W. Bush|     54|   AS|
| AND|             Andorra|       Europe|     Southern Europe|       468.0|      1278|     78000|           83.5|  1630.00|     NULL|             Andorra|Parliamentary Cop...|                    |     55|   AD|
| AGO|              Angola|       Africa|      Central Africa|   1246700.0|      1975|  12878000|           38.3|  6648.00|  7984.00|              Angola|            Republic|José Eduardo dos ...|     56|   AO|
| AIA|            Anguilla|North America|           Caribbean|        96.0|      NULL|      8000|           76.1|    63.20|     NULL|            Anguilla|Dependent Territo...|        Elisabeth II|     62|   AI|
| ATG| Antigua and Barbuda|North America|           Caribbean|       442.0|      1981|     68000|           70.5|   612.00|   584.00| Antigua and Barbuda|Constitutional Mo...|        Elisabeth II|     63|   AG|
| ARE|United Arab Emirates|         Asia|         Middle East|     83600.0|      1971|   2441000|           74.1| 37966.00| 36846.00|Al-Imarat al-´Ara...|  Emirate Federation|Zayid bin Sultan ...|     65|   AE|
| ARG|           Argentina|South America|       South America|   2780400.0|      1816|  37032000|           75.1|340238.00|323310.00|           Argentina|    Federal Republic|  Fernando de la Rúa|     69|   AR|
| ARM|             Armenia|         Asia|         Middle East|     29800.0|      1991|   3520000|           66.4|  1813.00|  1627.00|            Hajastan|            Republic|    Robert Kotšarjan|    126|   AM|
| ABW|               Aruba|North America|           Caribbean|       193.0|      NULL|    103000|           78.4|   828.00|   793.00|               Aruba|Nonmetropolitan T...|             Beatrix|    129|   AW|
| AUS|           Australia|      Oceania|Australia and New...|   7741220.0|      1901|  18886000|           79.8|351182.00|392911.00|           Australia|Constitutional Mo...|        Elisabeth II|    135|   AU|
| AZE|          Azerbaijan|         Asia|         Middle East|     86600.0|      1991|   7734000|           62.9|  4127.00|  4100.00|          Azärbaycan|    Federal Republic|       Heydär Äliyev|    144|   AZ|
| BHS|             Bahamas|North America|           Caribbean|     13878.0|      1973|    307000|           71.1|  3527.00|  3347.00|         The Bahamas|Constitutional Mo...|        Elisabeth II|    148|   BS|
| BHR|             Bahrain|         Asia|         Middle East|       694.0|      1971|    617000|           73.0|  6366.00|  6097.00|          Al-Bahrayn|  Monarchy (Emirate)|Hamad ibn Isa al-...|    149|   BH|
| BGD|          Bangladesh|         Asia|Southern and Cent...|    143998.0|      1971| 129155000|           60.2| 32852.00| 31966.00|          Bangladesh|            Republic|   Shahabuddin Ahmad|    150|   BD|
| BRB|            Barbados|North America|           Caribbean|       430.0|      1966|    270000|           73.0|  2223.00|  2186.00|            Barbados|Constitutional Mo...|        Elisabeth II|    174|   BB|
+----+--------------------+-------------+--------------------+------------+----------+----------+---------------+---------+---------+--------------------+--------------------+--------------------+-------+-----+
only showing top 20 rows

+----+--------------------+---------+--------------------+------------+----------+----------+---------------+----------+----------+--------------------+--------------------+--------------------+-------+-----+
|code|                name|continent|              region|surface_area|indep_year|population|life_expectancy|       gnp|   gnp_old|          local_name|     government_form|       head_of_state|capital|code2|
+----+--------------------+---------+--------------------+------------+----------+----------+---------------+----------+----------+--------------------+--------------------+--------------------+-------+-----+
| AFG|         Afghanistan|     Asia|Southern and Cent...|    652090.0|      1919|  22720000|           45.9|   5976.00|      NULL|Afganistan/Afqane...|     Islamic Emirate|       Mohammad Omar|      1|   AF|
| ARE|United Arab Emirates|     Asia|         Middle East|     83600.0|      1971|   2441000|           74.1|  37966.00|  36846.00|Al-Imarat al-´Ara...|  Emirate Federation|Zayid bin Sultan ...|     65|   AE|
| ARM|             Armenia|     Asia|         Middle East|     29800.0|      1991|   3520000|           66.4|   1813.00|   1627.00|            Hajastan|            Republic|    Robert Kotšarjan|    126|   AM|
| AZE|          Azerbaijan|     Asia|         Middle East|     86600.0|      1991|   7734000|           62.9|   4127.00|   4100.00|          Azärbaycan|    Federal Republic|       Heydär Äliyev|    144|   AZ|
| BHR|             Bahrain|     Asia|         Middle East|       694.0|      1971|    617000|           73.0|   6366.00|   6097.00|          Al-Bahrayn|  Monarchy (Emirate)|Hamad ibn Isa al-...|    149|   BH|
| BGD|          Bangladesh|     Asia|Southern and Cent...|    143998.0|      1971| 129155000|           60.2|  32852.00|  31966.00|          Bangladesh|            Republic|   Shahabuddin Ahmad|    150|   BD|
| BTN|              Bhutan|     Asia|Southern and Cent...|     47000.0|      1910|   2124000|           52.4|    372.00|    383.00|            Druk-Yul|            Monarchy|Jigme Singye Wang...|    192|   BT|
| BRN|              Brunei|     Asia|      Southeast Asia|      5765.0|      1984|    328000|           73.6|  11705.00|  12460.00|   Brunei Darussalam|Monarchy (Sultanate)|Haji Hassan al-Bo...|    538|   BN|
| PHL|         Philippines|     Asia|      Southeast Asia|    300000.0|      1946|  75967000|           67.5|  65107.00|  82239.00|           Pilipinas|            Republic|Gloria Macapagal-...|    766|   PH|
| GEO|             Georgia|     Asia|         Middle East|     69700.0|      1991|   4968000|           64.5|   6064.00|   5924.00|          Sakartvelo|            Republic|  Eduard Ševardnadze|    905|   GE|
| HKG|           Hong Kong|     Asia|        Eastern Asia|      1075.0|      NULL|   6782000|           79.5| 166448.00| 173610.00| Xianggang/Hong Kong|Special Administr...|         Jiang Zemin|    937|   HK|
| IDN|           Indonesia|     Asia|      Southeast Asia|   1904569.0|      1945| 212107000|           68.0|  84982.00| 215002.00|           Indonesia|            Republic|   Abdurrahman Wahid|    939|   ID|
| IND|               India|     Asia|Southern and Cent...|   3287263.0|      1947|1013662000|           62.5| 447114.00| 430572.00|        Bharat/India|    Federal Republic|Kocheril Raman Na...|   1109|   IN|
| IRQ|                Iraq|     Asia|         Middle East|    438317.0|      1932|  23115000|           66.5|  11500.00|      NULL|            Al-´Iraq|            Republic|Saddam Hussein al...|   1365|   IQ|
| IRN|                Iran|     Asia|Southern and Cent...|   1648195.0|      1906|  67702000|           69.7| 195746.00| 160151.00|                Iran|    Islamic Republic|Ali Mohammad Khat...|   1380|   IR|
| ISR|              Israel|     Asia|         Middle East|     21056.0|      1948|   6217000|           78.6|  97477.00|  98577.00|    Yisra’el/Isra’il|            Republic|        Moshe Katzav|   1450|   IL|
| TMP|          East Timor|     Asia|      Southeast Asia|     14874.0|      NULL|    885000|           46.0|      0.00|      NULL|         Timor Timur|Administrated by ...|José Alexandre Gu...|   1522|   TP|
| JPN|               Japan|     Asia|        Eastern Asia|    377829.0|      -660| 126714000|           80.7|3787042.00|4192638.00|        Nihon/Nippon|Constitutional Mo...|             Akihito|   1532|   JP|
| YEM|               Yemen|     Asia|         Middle East|    527968.0|      1918|  18112000|           59.8|   6041.00|   5729.00|            Al-Yaman|            Republic|  Ali Abdallah Salih|   1780|   YE|
| JOR|              Jordan|     Asia|         Middle East|     88946.0|      1946|   5083000|           77.4|   7526.00|   7051.00|           Al-Urdunn|Constitutional Mo...|         Abdullah II|   1786|   JO|
+----+--------------------+---------+--------------------+------------+----------+----------+---------------+----------+----------+--------------------+--------------------+--------------------+-------+-----+
only showing top 20 rows
```
