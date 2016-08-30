/* TPCDSBenchmark.scala */
import scala.collection.mutable
import scala.concurrent.duration._

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext

import com.databricks.spark.sql.perf.tpcds.Tables
import com.databricks.spark.sql.perf.tpcds.TPCDS


object TPCDSBenchmark {

  /*
   * Options:
   *   -gendata generate the database using dsdgen
   *   -sf scale factor
   *   -dsdgen path to the dsdgen tools directory
   *   -iter number of iterations to run
   *   -location where is (generated) the database, might be huge
   */
  def main(args: Array[String]) {
    val options = args.map {
      arg =>
        arg.dropWhile(_ == '-').split('=') match {
          case Array(opt, v) => (opt -> v)
          case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
        }
    }

    var location = System.getProperty("user.home") + File.separator + ".cache/tpc-ds/"
    var scalef = 1
    var dsdgen = System.getProperty("user.home") + File.separator + "git/tpcds-kit/tools/"
    var gendata = true
    var iter = 1

    options.foreach {
      case ("location", v) => location = v
      case ("sf", v) => scalef = v.toInt
      case ("gendata", v) => gendata = v.toBoolean
      case ("dsdgen", v) => dsdgen = v
      case ("iter", v) => iter = v.toInt
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }

    val conf = new SparkConf().setAppName("TPCDS Benchmark")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    sqlContext.setConf("spark.sql.perf.results", "results")

    val tables = new Tables(sqlContext, dsdgen, scalef)

    if (gendata) {
      println("Generating TPC-DS data.")
      tables.genData(location, "parquet", true, true, false, false, false)
    }

    println(s"Create temporary tables.")
    tables.createTemporaryTables(location, "parquet")

    val tpcds = new TPCDS()

    println(s"Run experiment.")
    val queries = tpcds.sqlDialectRunnable
    val experiment = tpcds.runExperiment(queries, iterations = iter)
    experiment.waitForFinish(3600*10)
  }
}
