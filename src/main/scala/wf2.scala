import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

object wf2 {

  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
     sparkconf.set("spark.appname", "windowfunctions")
     sparkconf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
         .config(sparkconf)
         .getOrCreate()


    import spark.implicits._

    //INCLUDES RANK & DENSE RANK PROBLEMS

    //problem 1
    // Employee Productivity Comparison

    //Determine the quarterly ranking of employees based on tasks completed.
    //Make sure employees with the same number of tasks completed share the same rank,
    //and use a ranking function that can handle ties without skipping ranks.

    /*val productivityData = Seq(
      ("Karthik", "2023-Q1", 120),
      ("Vijay", "2023-Q1", 150),
      ("Jay", "2023-Q1", 120),
      ("Mohan", "2023-Q1", 130),
      ("Karthik", "2023-Q2", 140),
      ("Vijay", "2023-Q2", 130),
      ("Jay", "2023-Q2", 160),
      ("Mohan", "2023-Q2", 120)
    ).toDF("employee", "quarter", "tasks_completed")

    productivityData.show()

    val window = Window.partitionBy("quarter").orderBy(col("tasks_completed"))

    val df = productivityData.withColumn("ranking",dense_rank().over(window)).show()

*/


    //problem 2

    //Monthly Expenditure Tracking

    //For each month, rank individuals based on their expenditure amount,
    //ensuring that each rank is sequential without any gaps.
    //Assign the same rank to individuals with the same expenditure amount and continue to the next rank accordingly.

   /* val expensesData = Seq(
      ("Karthik", "2023-01", 300),
      ("Vijay", "2023-01", 450),
      ("Jay", "2023-01", 450),
      ("Mohan", "2023-01", 300),
      ("Karthik", "2023-02", 500),
      ("Vijay", "2023-02", 400),
      ("Jay", "2023-02", 500),
      ("Mohan", "2023-02", 300)
    ).toDF("person", "month", "amount")
    expensesData.show()

    val window = Window.partitionBy("month").orderBy("amount")
    val df = expensesData.withColumn("ranking", dense_rank().over(window)).show()*/


    //problem 3

    // Student Grade Evaluation

    //For each subject, rank students by their scores.
    //Make sure that students with identical scores have the same rank, and the next rank should increment sequentially.

/*    val gradesData = Seq(
      ("Karthik", "Math", 78),
      ("Vijay", "Math", 85),
      ("Jay", "Math", 85),
      ("Mohan", "Math", 78),
      ("Karthik", "Science", 82),
      ("Vijay", "Science", 80),
      ("Jay", "Science", 90),
      ("Mohan", "Science", 85)
    ).toDF("student", "subject", "score")
    gradesData.show()

   val window = Window.partitionBy("subject").orderBy("score")
    val df = gradesData.withColumn("ranking", dense_rank().over(window))
    df.show()*/


    //problem 4
    //Store Performance Ranking by Monthly Sales

    //Rank the stores based on monthly sales for each manager.
    //For stores with the same sales in a given month,
    //they should receive the same rank, but each manager’s ranking should start at 1 for every month.

    val storeSalesData = Seq(
      ("Karthik", "Store_A", "2023-03", 1500),
      ("Vijay", "Store_B", "2023-03", 1500),
      ("Jay", "Store_C", "2023-03", 1800),
      ("Mohan", "Store_D", "2023-03", 1600),
      ("Karthik", "Store_A", "2023-04", 2000),
      ("Vijay", "Store_B", "2023-04", 1800),
      ("Jay", "Store_C", "2023-04", 2000),
      ("Mohan", "Store_D", "2023-04", 1800)
    ).toDF("manager", "store", "month", "sales")

    storeSalesData.show()

    val window = Window.partitionBy("manager").orderBy("sales")
    val df =   storeSalesData.withColumn("ranking",dense_rank().over(window))
    df.show()
    storeSalesData.printSchema()


  }
}
