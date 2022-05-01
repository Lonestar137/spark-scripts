import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame

val hiveContext= new HiveContext(sc)



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

object HiveHandler{
    //TODO: add a printSchema check function to make sure that the table has Date, Open, Close etc.  . . before entering function.
    //Add the function to each function so that it checks the RDD
    
    //1. write a function finds standard deviation between two stock tables with same schema.
    def standardDeviation(table1: String="wmt", table2: String="tgt") : DataFrame =
    {
        val result =hiveContext.sql("SELECT STDDEV_POP("+table1+".close) AS `"+table1+" Historical Volatility`, STDDEV_POP("+table2+".close) AS `"+table2+" Historical Volatility` FROM "+table1+", "+table2+" WHERE "+table1+".`date`="+table2+".`date`")
        return result;
    }

    //2. Write a function that finds the months of a year in which Walmart has the highest volatility.
    def highestVolatility(table: String="wmt") : DataFrame=
    {
        val result = hiveContext.sql("SELECT SQRT(POWER('Close' - (SELECT AVG('Close') AS average FROM wmt), 2)) AS deviation, 'Date' FROM wmt GROUP BY 'Date' ORDER BY deviation DESC")
        return result;
    }

    //3. Write a function that finds the beta value of Walmart
    def betaValue(stock: String="wmt", index: String="sp500") : DataFrame=
    {
        val result = hiveContext.sql("SELECT (COVAR_POP("+stock+".close, "+index+".close) / VAR_POP("+index+".close)) AS beta FROM "+stock+", "+index+" WHERE "+stock+".date = "+index+".date;")
        return result;
    }

    //TODO continue refactor
    //4a Function that finds the dividend of a company over years
    def dividendOver50(stock: String="wmt", stock_div: String="wmt_div") : DataFrame=
    {
        val result = hiveContext.sql(s"SELECT AVG(div_yield) AS `dividend yield` FROM(SELECT ($stock_div.dividends/$stock.close) AS `div_yield` FROM $stock, $stock_div WHERE $stock.`date`=$stock_div.`date`)$stock, $stock_div")
        return result;
    }

    //4b Write a function that finds if dividend has grown or decline
    def dividendOverYears(stock: String="wmt", stock_div: String="wmt_div") : DataFrame=
    {
        val result = hiveContext.sql(s"SELECT $stock.`date`, ($stock_div.dividends/$stock.close) AS `dividend yield` FROM $stock, $stock_div WHERE $stock.`date` = $stock_div.`date` ORDER BY $stock.`date` DESC")
        return result;
    }

    //5 What is the 95% VaR of Walmart and Target
    def VaR(stock: String="wmt") : DataFrame=
    {
        val result = hiveContext.sql(s"SELECT (1.65 * STDDEV_POP($stock.close)) AS `VaR 95%`, (2.33 * STDDEV_POP($stock.close)) AS `VaR 99%` FROM $stock")
        return result;
    }

    //==No longer used==
    //6a What is the D/E (long_term_debt/stockholders_equity) ratio for Walmart over the years? 
    def ratioOverYears() : DataFrame=
    {
        val result = hiveContext.sql("SELECT `date`, (long_term_debt/stockholders_equity) AS `D/E Ratio` FROM wmt_annual_bal_sht ORDER BY `date` DESC")
        return result;
    }

    //6b What is the average D/E ratio and is the current 2021 D/E ratio beyond normal limits (std(?
    def ratioBeyondLimits() : DataFrame=
    {
        val result = hiveContext.sql("SELECT AVG(long_term_debt/stockholders_equity) AS `Avg D/E Ratio`, STDDEV_POP(long_term_debt/stockholders_equity) AS `Std Dev of D/E Ratio` FROM wmt_annual_bal_sht")
        return result;
    }

}

spark.sql("use blue_team")
spark.conf.set("spark.sql.crossJoin.enabled", true)

//Load the tables into a DF
val tgt = spark.sql("select * from tgt").where(col("`date`").isNotNull)
val tgt_div = spark.sql("select * from tgt_div").where(col("`date`").isNotNull)

val wmt = spark.sql("select * from wmt").where(col("`date`").isNotNull)
val wmt_div = spark.sql("select * from wmt_div").where(col("`date`").isNotNull)


