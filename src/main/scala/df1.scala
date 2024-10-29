import org.apache.spark.sql.SparkSession

object df1 {
  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", true)
      .option("path", "E:/data/details.csv")
      .load()
    df.show()
    df.printSchema()








  }

}
