# Description

This project is a demonstration of Apache Spark, Scala, JDBC and Hive
automatic database configuraton.  The scripts inside this project are intended to be used individually inside the spark-shell (spark-shell -i [script.scala]).

Custom Scala scripts can easily be written and loaded into Spark-shell.  Inside there is also an example of a .SQL file which is loaded into the dbsetup.scala file and used to create a 
database from a set of csv files automatically.

### Getting started

1. Clone this repo to your Hive server or remember the IP:jdbc_port of your Hive server.
2. Run the command: `./setup.sh`. You will need to provide your Hive credentials and the IP+port for a JDBC connection. 
3. A new database `stockmart` will be created inside your Hive server.  

### Technologies
- Apache Spark
- Spark SQL
- HDFS 
- JDBC
- SBT
- Scala
- Git + GitHub
- JIRA

### Colaborators
* Andre Xie
* Mohammad Aydin
* Ed Osmulski
* Garrett Jones
* Max Landa

### License

This project uses the following [License](<https://github.com/Lonestar137/spark-scripts/blob/add-license-1/LICENSE>).

 
