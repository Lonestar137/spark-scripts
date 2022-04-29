import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame



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


val hiveContext= new HiveContext(sc)
spark.sql("use blue_team")
spark.conf.set("spark.sql.crossJoin.enabled", true)
//1. write a function finds standard deviation between two stock tables with same schema.
def printDev() : DataFrame =
{
    val result =hiveContext.sql("SELECT STDDEV_POP(wmt.close) AS `Walmart Historical Volatility`, STDDEV_POP(tgt.close) AS `Target Historical Volatility` FROM wmt, tgt WHERE wmt.`date`=tgt.`date`")
    return result;
}
//2. Write a function that finds the months of a year in which Walmart has the highest volatility.
/*
def highestVolatility() : DataFrame=
{
    val result = hiveContext.sql("SELECT SQRT(POWER('Close' - (SELECT AVG('Close') AS average FROM walmart), 2)) AS deviation, 'Date' FROM walmart GROUP BY 'Date' ORDER BY deviation DESC")
    return result;
}
*/

//3. Write a function that finds the beta value of Walmart
/*
def betValue() : DataFrame=
{
    val result = hiveContext.sql("SELECT (COVAR_POP(wmt.close, sp500.close) / VAR_POP(sp500.close)) AS beta FROM wmt, sp500 WHERE wmt.date = sp500.date;
")
    return result;
}
*/

//4a Write a function that finds the dividend of walmart over years
def dividendOver50() : DataFrame=
{
    val result = hiveContext.sql("SELECT AVG(div_yield) AS `dividend yield` FROM(SELECT (wmt_div.dividends/wmt.close) AS `div_yield` FROM wmt, wmt_div WHERE wmt.`date`=wmt_div.`date`)wmt, wmt_div")
    return result;
}

//4b Write a function that finds if dividend has grown or decline
def dividendOverYears() : DataFrame=
{
    val result = hiveContext.sql("SELECT wmt.`date`, (wmt_div.dividends/wmt.close) AS `dividend yield` FROM wmt, wmt_div WHERE wmt.`date` = wmt_div.`date` ORDER BY wmt.`date` DESC")
    return result;
}

//5 What is the 95% VaR of Walmart and Target
def VaR() : DataFrame=
{
    val result = hiveContext.sql("SELECT (1.65 * STDDEV_POP(wmt.close)) AS `VaR 95%`, (2.33 * STDDEV_POP(wmt.close)) AS `VaR 99%` FROM wmt")
    return result;
}

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

//Load the tables into a DF
val tgt = spark.sql("select * from tgt")
val tgt_div = spark.sql("select * from tgt_div")

val wmt = spark.sql("select * from wmt")
val wmt_div = spark.sql("select * from wmt_div")




