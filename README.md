# Description

This project is a demonstration of Apache Spark, Scala, JDBC and Hive
automatic database configuraton.  The scripts inside this project are intended to be used individually inside the spark-shell (spark-shell -i [script.scala]).

# Getting started
Custom Scala scripts can easily be written and loaded into Spark-shell.  Inside there is also an example of a .SQL file which is loaded into the dbsetup.scala file and used to create a 
database from a set of csv files automatically.

### Getting started

1. Clone this repo to your Hive server or start the spark shell.
2. Run the command: `./start.sh`. You will need to provide your Hive credentials and the IP+port for a JDBC connection you can do so by editing the load.scala script or writing your own. 

### Technologies
- Apache Spark
- Spark SQL
- HDFS 
- JDBC
- SBT
- Scala
- Git + GitHub
- JIRA

### Collaborators
* Andre Xie
* Mohammad Aydin
* Ed Osmulski
* Garrett Jones
* Max Landa

### License

This project uses the following [License](<https://github.com/Lonestar137/spark-scripts/blob/main/LICENSE>).

 
