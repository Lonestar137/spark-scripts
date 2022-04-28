import org.apache.spark.sql.hive.HiveContext




def printRed(str: String, newLine: Boolean = true) = {
  if (newLine) {
    Console.println("\u001b[31m" + str + "\u001b[0m")
  } else {
    Console.print("\u001b[31m" + str + "\u001b[0m")
  }
}
def printYellow(str: String, newLine: Boolean = true) = {
  if (newLine){
    Console.println("\u001b[33m" + str + "\u001b[0m")
  } else {
    Console.print("\u001b[33m" + str + "\u001b[0m")
  }
}
def printGreen(str: String, newLine: Boolean = true) = {
  if (newLine){
    Console.println("\u001b[32m" + str + "\u001b[0m")
  } else {
    Console.print("\u001b[33m" + str + "\u001b[0m")
  }
}

//1. write a function finds standard deviation between two stock tables with same schema.
//2. Write a function that finds the months of a year in which Walmart has the highest volatility.


val hiveContext= new HiveContext(sc)

spark.sql("use blue_team")


//Load the tables into a DF
val tgt = spark.sql("select * from tgt")
val tgt_div = spark.sql("select * from tgt_div")

val wmt = spark.sql("select * from wmt")
val wmt_div = spark.sql("select * from wmt_div")




