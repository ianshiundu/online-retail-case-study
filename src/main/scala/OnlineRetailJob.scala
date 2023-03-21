import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class OnlineRetailJob(spark: SparkSession) {

  /**
   * We define the schema for the online retail data frame
   * */
  val onlineRetailSchema: StructType = StructType(Array(
    StructField("Invoice", StringType, nullable = false),
    StructField("StockCode", IntegerType, nullable = false),
    StructField("Description", StringType, nullable = false),
    StructField("Quantity", IntegerType, nullable = false),
    StructField("InvoiceDate", DateType, nullable = false),
    StructField("Price", DoubleType, nullable = false),
    StructField("Customer ID", IntegerType, nullable = false),
    StructField("Country", StringType, nullable = false)))

  // An ideal way would be to have this in an application.conf but since this code won't run in multiple environments this should suffice for now
  private val inputPath = "online_retail_II.xlsx"

  /**
   * The corpus from [[https://archive-beta.ics.uci.edu/dataset/502/online+retail+ii]]
   * was an excel file, I used a library to load the excel file from [[https://github.com/crealytics/spark-excel]]
   * You can check out the documentation to understand the configuration below
   * However, the most important config is maxByteArraySize the default setting is too small, so bumping it up ensures
   * we can read big excel files
   * */
  def loadFromFile(): DataFrame = {
    spark
      .read
      .format("excel")
      .option("dateFormat", "dd/MM/yyyy HH:mm")
      .option("useHeader", "true")
      .option("maxByteArraySize", 2147483647)
      .option("maxRowsInMemory", 10000000)
      .option("tempFileThreshold", 100000000)
      .option("header", "true")
      .option("sheetName", "Year 2009-2010,Year 2010-2011")
      .schema(onlineRetailSchema)
      .load(inputPath)

      // Clean the dataset by removing any rows with missing or invalid values.
      .na
      .drop() // drop rows with empty values
      .filter("Quantity > 0") // filter out cancelled orders
      .cache() // prevent a re-read each time we are transforming data using this DF
  }

  /**
   * The getRevenue DataFrame is re-used in other Data Frames thus having it as a function to call it more than once
   * This DF, adds a Revenue column
   * Approach:
   * - Create a new column and call it ["Revenue"]
   * - Revenue is a result of a multiplying col("Quantity") * col("Price")
   * */
  private def getRevenueDF: DataFrame = {
    val inputDF = loadFromFile()
    inputDF
      .withColumn("Revenue", round(col("Quantity") * col("Price")))
  }

  /**
   * We calculate the Total Revenue for each Stock price.
   * Approach:
   * - The data is grouped by StockCode to get the Revenue by StockCode
   * - We aggregate the Revenue by StockCode to get the total revenue for each stock code
   * - We round off the revenue price to 2 d.p.
   * Sample output:
   * +---------+-------------+
   *  |StockCode|Total Revenue|
   *  +---------+-------------+
   *  |    22423|     144184.0|
   *  |    84879|      70775.0|
   *  |    21843|      41815.0|
   * */
  def calculateProductRevenueDF: DataFrame = {
    getRevenueDF
      .groupBy("StockCode")
      .agg(round(sum("Revenue"), 2).alias("Total Revenue"))
      .sort(col("Total Revenue").desc) // sort revenue in descending order
  }

  /**
   * 4(a): We compute the average revenue per product category
   * Approach:
   * - We extract the ProductCategory column from the StockCodeByRevenue DataFrame
   * - The ProductCategory Column is a result of the first 3 digits of the StockCode thus substring(col("StockCode"), 0, 3)
   * - We then group by ProductCategory column so that we can aggregate the average revenue i.e. avg("Total Revenue")
   * Sample output:
   * +---------------+---------------+
   *  |ProductCategory|Average Revenue|
   *  +---------------+---------------+
   *  |            824|        19615.0|
   *  |            714|        13406.0|
   *  |            481|       12944.27|
   * */
  def getProductsAverageRevenueDF: Dataset[Row] = {
    getRevenueDF
      .withColumn("ProductCategory", substring(col("StockCode"), 0, 3)) // the first three characters of the StockCode represent the product category.
      .groupBy("ProductCategory")
      .agg(round(avg("Revenue"), 2).alias("Average Revenue"))
      .sort(col("Average Revenue").desc) // sort average revenue in descending order
  }

  /**
   * Get the total revenue by month.
   * Approach:
   * - Add a new column to the revenueDF called [InvoiceMonth] which is a formatted date from [InvoiceDate] i.e. date_format(col("InvoiceDate"), "yyyy-MM")
   * - We get the Revenue like we did for the StockCodes
   * - We then group the DF by InvoiceMonth
   * - Lastly we aggregate the total revenue in a given month and round it of to 2.d.p
   * Sample output:
   *  +------------+-------------+
   *  |InvoiceMonth|Total Revenue|
   *  +------------+-------------+
   *  |     2009-12|     507392.0|
   *  |     2010-01|     414170.0|
   *  |     2010-02|     411066.0|
   * */
  def getMonthlyRevenue: DataFrame = {
    getRevenueDF
      .withColumn("InvoiceMonth", date_format(col("InvoiceDate"), "yyyy-MM"))
      .groupBy("InvoiceMonth")
      .agg(round(sum("Revenue"), 2).alias("Total Revenue"))
      .orderBy(asc("InvoiceMonth")) // order invoice month column in chronological order
  }

  /**
   * Find the top 10 most popular products based on the total quantity sold
   * Approach:
   * - We group the input data frame by StockCode and Description (for context of the product)
   * - We aggregate the total quantity by the grouped data
   * - Sort the TotalQuantity by descending order so that we can limit 10 results to get the top 10 most popular products
   * Sample output:
   * +---------+--------------------+-------------+
   *  |StockCode|         Description|TotalQuantity|
   *  +---------+--------------------+-------------+
   *  |    84077|WORLD WAR 2 GLIDE...|        54754|
   *  |    17003| BROCADE RING PURSE |        48166|
   *  |    21212|PACK OF 72 RETRO ...|        45156|
   * */
  def getTop10MostPopularProductsByQuantityDF: Dataset[Row] = {
    val inputDF = loadFromFile()

    inputDF
      .groupBy("StockCode", "Description")
      .agg(sum("Quantity").alias("TotalQuantity"))
      .sort(col("TotalQuantity").desc)
      .limit(10) // get the top 10 most popular products

  }

  /**
   * APPENDIX:
   * - Apache Spark is not very good at reading excel files, the corpus was from an excel file thus used spark-excel
   * - Assumption made: we are not interested in getting revenue for cancelled items thus filtering out the Stock that had negative Quantities
   * - The whole job is within the default spark resources you can monitor the resources from http://localhost:4040/executors/
   * when the job is running. The only optimization that was necessary was caching the read excel file because it's an
   * expensive operation.
   * - One upside of not inferring a schema when using spark-excel is that we won't have to read through the whole
   * dataset. This significantly reduced the job time by making the schema explicit  i.e. onlineRetailSchema
   * */

}
