import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day22BatchPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day22-Ecommerce-Batch-Pipeline")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val transactions = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .csv("data/transactions.csv")
      .withColumn("quantity", col("quantity").cast("int"))
      .withColumn(
        "transaction_date",
        to_date(col("transaction_date"), "yyyy-MM-dd")
      )

    val customers = spark.read
      .option("header", "true")
      .csv("data/customers.csv")

    val products = spark.read
      .option("header", "true")
      .csv("data/products.csv")
      .withColumn("unit_price", col("unit_price").cast("double"))

    println("\n========== RAW TRANSACTIONS ==========")
    transactions.show(5, false)

    val rawCount = transactions.count()
    println(s"Raw transactions: $rawCount")

    val cleaned = transactions.filter(
      col("transaction_id").isNotNull &&
      length(trim(col("transaction_id"))) > 0 &&
      col("customer_id").isNotNull &&
      length(trim(col("customer_id"))) > 0 &&
      col("product_id").isNotNull &&
      length(trim(col("product_id"))) > 0 &&
      col("quantity").isNotNull &&
      col("quantity") > 0 &&
      col("transaction_date").isNotNull
    )

    val cleanedCount = cleaned.count()

    println("\n========== DATA CLEANING ==========")
    println(s"Valid transactions: $cleanedCount")
    println(s"Invalid transactions removed: ${rawCount - cleanedCount}")

    val validCustomers = customers
      .filter(
        col("customer_id").isNotNull &&
        col("customer_name").isNotNull &&
        col("city").isNotNull
      )
      .dropDuplicates("customer_id")

    val validProducts = products
      .filter(
        col("product_id").isNotNull &&
        col("product_name").isNotNull &&
        col("category").isNotNull &&
        col("unit_price").isNotNull &&
        col("unit_price") >= 0
      )
      .dropDuplicates("product_id")

    val enriched = cleaned
      .join(validCustomers, Seq("customer_id"), "inner")
      .join(validProducts, Seq("product_id"), "inner")
      .withColumn("revenue", col("quantity") * col("unit_price"))
      .withColumn("year", year(col("transaction_date")))
      .withColumn("month", month(col("transaction_date")))
      .withColumn("day", dayofmonth(col("transaction_date")))

    println("\n========== ENRICHED SALES ==========")
    enriched.select(
      "transaction_id",
      "customer_name",
      "city",
      "product_name",
      "category",
      "quantity",
      "unit_price",
      "revenue",
      "transaction_date"
    ).show(30, false)

    println(s"Transactions after customer/product joins: ${enriched.count()}")

    val dailySales = enriched
      .groupBy(
        "transaction_date",
        "city",
        "category",
        "year",
        "month",
        "day"
      )
      .agg(
        countDistinct("transaction_id").as("total_orders"),
        sum("quantity").as("total_quantity"),
        round(sum("revenue"), 2).as("total_revenue")
      )

    println("\n========== DAILY SALES SUMMARY ==========")
    dailySales
      .orderBy("transaction_date", "city", "category")
      .show(100, false)

    val outputPath = "output/daily_sales"

    dailySales.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(outputPath)

    println(s"Parquet saved to: $outputPath")

    val savedSales = spark.read.parquet(outputPath)

    println("\n========== VERIFIED PARQUET OUTPUT ==========")
    savedSales.orderBy("transaction_date").show(100, false)
    println(s"Saved aggregate rows: ${savedSales.count()}")

    println("\n========== PIPELINE COMPLETED ==========")

    spark.stop()
  }
}
