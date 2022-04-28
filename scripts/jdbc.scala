import org.apache.hive.jdbc.HiveDriver
import java.sql.DriverManager
import java.sql.Connection
import scala.annotation.meta.setter
import java.sql.ResultSetMetaData


def jd(table: String)={
    val df = spark.read.format("jdbc")
        .option("url", "jdbc:hive2://publicsandbox:10000/stockmart")
        .option("dbtable", table)
        .option("user", "hive")
        .option("password", "hive")
        .option("header", "true")
        .option("inferSchema", "true")
        .load()
    df
}
val df = spark.read.format("jdbc")
    .option("url", "jdbc:hive2://publicsandbox:10000/test")
    .option("dbtable", "tgt")
    .option("user", "hive")
    .option("password", "hive")
    .load()

//val dx = spark.read.format("jdbc")
//    .option("url", "jdbc:hive2://publicsandbox:10016/test")
//    .option("dbtable", "test")
//    .option("user", "hive")
//    .option("password", "hive")
//    .load()
