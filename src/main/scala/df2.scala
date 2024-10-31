import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import sun.misc.MessageUtils.where

object df2 {
  def main(args:Array[String]):Unit={

    val sparkconf=new SparkConf()
    sparkconf.set("spark.app.master","spark-program")
    sparkconf.set("spark.master","local[*]")

    val spark=SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val schema =StructType(List(
      StructField("id",IntegerType),
      StructField("Name",StringType),
      StructField("Salary",IntegerType),
      StructField("City",StringType)

    ))
    val df = spark.read.format("csv")
      .option("header", true)
      .schema(schema)
      //.option("mode","PERMISSIVE") //default it takes, no need to specify, it makes whole row as null
      //.option("mode" ,"DROPMALFORMED") // it removes that particular row from your data
      //.option("mode","FAILFAST") //it throws an error
      .option("path", "E:/data/details.csv")
      .load()

//    df.printSchema()
//    df.show()

    //Spark DSL

    /*df.select(

      col("id"),
      col("Name"),
      col("Salary"),
      col("City"),
      when(col("Salary")>800,"rich").when(col("Salary")>400 && col("Salary")<=800,"middle").otherwise("poor").alias("status")
    ).show()*/

    // using sql spark

   /* df.createTempView("vijay")
    spark.sql(
      """
        select
        id,
        Name,
        Salary,
        City,
        Case
        when Salary > 800 then "Rich"
        when Salary > 400 and Salary <= 800 then "MIDDLE"
        else "POOR"
        end as status
        from
        vijay
        """
    ).show()*/

    //withColumn

  /*df.withColumn("status",
    when(col("Salary")>800,"rich").when(col("Salary")>400 && col("Salary")<=800,"middle").otherwise("poor"))
      .withColumn("category", when(col("id")>5, "senior").otherwise("junior")
  ).show()
*/

  }

}
