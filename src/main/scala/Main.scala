import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OnlineRetail")
      .config("spark.master", "local")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.cores", "4")
      .getOrCreate()

    val onlineRetailJob = new OnlineRetailJob(spark)

    /**
     * Show the results of the Data Frames in STDOUT
     * */
    println(s"*"*10 + "TOTAL REVENUE FOR EACH STOCK" + "*"*10)
    println(onlineRetailJob.calculateProductRevenueDF.show(50)) // feel free to update this e.g show(100) to output more data

    println(s"*" * 10 + "PRODUCTS BY AVERAGE REVENUE" + "*" * 10)
    println(onlineRetailJob.getProductsAverageRevenueDF.show(50))

    println(s"*" * 10 + "MONTHLY REVENUE BY MONTH" + "*" * 10)
    println(onlineRetailJob.getMonthlyRevenue.show())

    println(s"*" * 10 + "TOP 10 MOST POPULAR PRODUCTS BY QUANTITIES SOLD" + "*" * 10)
    println(onlineRetailJob.getTop10MostPopularProductsByQuantityDF.show())

    // un persist the input DF after transformation
    onlineRetailJob.loadFromFile().unpersist()

  }
}
