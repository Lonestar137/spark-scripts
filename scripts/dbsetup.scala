import org.apache.spark.sql.DataFrame

//jdbc
import org.apache.hive.jdbc.HiveDriver
import java.sql.DriverManager
import java.sql.Connection
import scala.annotation.meta.setter
import java.sql.ResultSetMetaData



trait OutputFunctions {
//Output formatting functions

    
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

}

object hive extends OutputFunctions {
    //jdbc setup 
    val driver = "org.apache.hive.jdbc.HiveDriver"
    var url = ""
    var username = "" //i.e "postgres"
    var password = ""
    var connection: Connection = null

    def setCreds(): Unit ={
        println("Set creds parameters: user, password, database, server:port")
    }

    def setCreds(newUser: String, newPassword: String, newDatabase: String="default", ini_url: String = "localhost:10000/") = {
      username = newUser
      password = newPassword
      url = "jdbc:hive2://"+ini_url +"/"+newDatabase
      // make the connection
      Class.forName(driver)

      //connect to hive database 
      connection = DriverManager.getConnection(url, username, password)
    }

    def executeQuery(query: String): Unit = {
        val statement = connection.createStatement()
        statement.executeQuery(query)
    }

    def show(query: String, print: Boolean = false, printLimit: Int = 20): Seq[Any] = {
  
      var swap = ""
      var select_split = query.split(" ")
      select_split.foreach(arg => {
        swap = arg.replace(",", "")
        select_split.drop(select_split.indexOf(arg))
        select_split.update(select_split.indexOf(arg), swap)
      })
  
      var columns = select_split.slice(1, select_split.indexOf("FROM"))
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(query)
      var rows = Seq[Object]()
      val rsMetaData = resultSet.getMetaData()
      val count = rsMetaData.getColumnCount()
      var rowNames = Seq[String]()
      val colName = ""
      for (i <- 1 to count)
      {
        var nextColName = colName
        nextColName += rsMetaData.getColumnName(i)
        //adds spacing to end of column name
        for (j <- nextColName.length to 16)
          {
            nextColName += " "
          }
          rowNames = rowNames :+ nextColName
      }
      if (print) {
        println(rowNames.toString.substring(4))
        println("---------------------------------------------------------------------------------------")
        while(resultSet.next()){
          var row = Seq[String]()
          for (i <- 1 to resultSet.getMetaData.getColumnCount) {
            val spacing = 16
            var dataString = "NULL"
            if (resultSet.getString(i) != null)
            {
              dataString = resultSet.getString(i)
            }
            var newDataString = dataString
            for (j <- dataString.length to spacing)
              {
                newDataString += " "
              }
            row = row :+ newDataString
  
          }
          rows = rows :+ row
        }
        //remove the "List" from start of each row
  
        rows.take(printLimit).foreach{ r => println(r.toString.substring(4))}
        println("---------------------------------------------------------------------------------------")
      }
      if(print){
        if(rows.length > printLimit){
          println("... and " + (rows.length-printLimit) + " more rows.")
        }
      }
  
      rows
    }


    //SPARK SQL functions
    def loadCSV(path:String): DataFrame={
        println("load data from "+path)
        val df = spark.read.format("csv").option("header","true").load(path)
        df
    }

    def loadTable(db: String, tablename: String): DataFrame={
        val df = spark.read.format("orc").option("inferSchema", true).option("headers", true).load("hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/"+db+".db/"+tablename)
        df
    }

    def test()={
        println("test")
    }
}


//call to webserver goes here.


//spark-shell script that asks for IP of your vm
//then jdbcs to hive, loads setup.sql to string and executes it.
//
//
//
//
//

val sql_string = scala.io.Source.fromFile("./sql/setup.sql").mkString
//get user input 
val user = scala.io.StdIn.readLine("Enter username: ")
val password = scala.io.StdIn.readLine("Enter password: ")
val server = scala.io.StdIn.readLine("Enter hive server and port(i.e. localhost:10000): ")
hive.setCreds(user, password, "default", server)
hive.executeQuery(sql_string)
println("Setup complete")



