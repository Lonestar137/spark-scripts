import org.apache.spark.sql.DataFrame

object query{
    // show best performing store for a quarter
    def getQuarterlyGrowth(year_num: Int, qrter: Int, tbl: DataFrame): DataFrame ={
        val result = tbl.filter(year(col("date")) === year_num && quarter(col("date")) === qrter).groupBy("date", "store").agg(
            sum("weekly_sales").cast("decimal(38,2)").as("total_sales"),
            avg("temperature").cast("decimal(4,2)").as("avg_temp"),
            avg("fuel_price").cast("decimal(38,3)").as("avg_fuel_price")
            ).orderBy(desc("total_sales"))
        return result
    }
}

val df = spark.read.format("csv").option("header", true).load("csv/WALMART_SALES_DATA.csv")
val sales = df.withColumn("Date", date_format(to_date(col("Date"),"dd-MM-yyyy"),"yyyy-MM-dd"))

object wmt_sales{
    val years = List(2010, 2011, 2012)
    val quarters = List(1, 2, 3, 4)
    var overall_sales = query.getQuarterlyGrowth(2010, 2, sales).limit(0)
    years.foreach( year => {
        quarters.foreach( qrter => {
            val result = query.getQuarterlyGrowth(year, qrter, sales).limit(1)
            overall_sales = overall_sales.union(result)
        })
    })
    overall_sales.show()
}


