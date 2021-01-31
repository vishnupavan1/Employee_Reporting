import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import com.bigdata.ex.Sample
import org.scalatest.funsuite.AnyFunSuite

class SampleSpec extends AnyFunSuite {

  val spark = SparkSession.builder().appName("Sample Spec Unit Testing").master("local").getOrCreate()

  test("Sample class Functionality ") {


    val input_df = spark.read.format("csv").option("header", true).option("inferschema", true).load("src\\resources\\employee.csv")
    val output_test_df = spark.read.format("csv").option("header", true).option("inferschema", true).load("src\\resources\\output.csv")
    output_test_df.show(10)

    val obj1 = new Sample()
    obj1.initialize()
    obj1.read()
    val output_df = obj1.process()
    val compare_df = output_df.except(output_test_df)

    assert(compare_df.count() == 0 )
  }

}
