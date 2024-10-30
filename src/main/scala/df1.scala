import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object df1 {
  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    //ddl approach
//    val schema = "id Int, name String, age Int"

   // programmatical approach
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age",IntegerType)

    ))
    val df = spark.read.format("csv")
      .option("header", true)
//      .option("inferschema",true)
      .schema(schema)
      .option("path", "E:/data/details.csv")
      .load()

    df.printSchema()
    df.show()









  }

}
