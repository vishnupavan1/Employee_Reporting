package com.bigdata.ex

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import com.bigdata.ex.Transformation
import org.apache.spark.sql.DataFrame

class Sample extends Transformation {

  /**
   * Sample Demonstration
   * Author :
   * Date : 10/01/2020
   */

  var spark: SparkSession = _
  var input_df:DataFrame =  _
  var output_df:DataFrame = _

  override def initialize(): SparkSession =
  {
    spark = SparkSession.builder().appName("Sample Demonstration").master("local").getOrCreate()
    return spark
  }

  override  def read():DataFrame = {

    input_df = spark.
      read.
      format("csv").
      option("inferSchema", true).
      option("header", true).
      load("src\\resources\\employee.csv")

    return input_df

  }

  override def process(): DataFrame =  {


    val dept_df1 = spark.
      read.
      format("csv").
      option("inferSchema", true).
      option("header", true).
      load("src\\resources\\department.csv")

    /*By Default sort merge joins will take place
       *
       * Since the two datasets are very small and broadcast hash join will takes place.
       *
       * if one of the dataset is small we can broadcast the cast and perfrom the join which will be
       * faster when compared to sort merge join
       *
       *
       * */

    val joined_df = input_df.as("emp").join(broadcast(dept_df1.as("dept")), col("emp.DEPARTMENT_ID") === col("dept.DEPARTMENT_ID"), "inner").
      drop(col("dept.DEPARTMENT_ID")).
      drop(col("dept.MANAGER_ID"))

    def concat_udf(first: String, last: String): String = {

      var full_name = first.concat(" ").concat(last)

      full_name

    }

    val concat_udf1 = udf(concat_udf _)
    val upperUDF = udf { s: String => s.toUpperCase }

    val final_df = joined_df.withColumn("Full_name", concat_udf1(col("FIRST_NAME"), col("LAST_NAME"))).
      withColumn("Upper_case",upperUDF(col("FIRST_NAME")))

    /* using the show function to execute the process
     * as the spark executes lazily
     *
     *
     *  */

    output_df = final_df
    return output_df
  }
  override def write() ={

    output_df.repartition(1).write.
      format("csv").
      mode("overWrite").
      option("header", true).
      save("C:\\Users\\vishn\\Desktop\\Hadoop\\output")
  }


}
