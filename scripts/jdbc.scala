
val df = spark.read.format("jdbc")
    .option("url", "jdbc:hive2://sandbox:10016/test")
    .option("dbtable", "test")
    .option("user", "hive")
    .option("password", "hive")
    .load()

val dx = spark.read.format("jdbc")
    .option("url", "jdbc:hive2://publicsandbox:10016/test")
    .option("dbtable", "test")
    .option("user", "hive")
    .option("password", "hive")
    .load()
