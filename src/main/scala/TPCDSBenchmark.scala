/* TPCDSBenchmark.scala */
import scala.collection.mutable
import scala.concurrent.duration._

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel


import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import com.databricks.spark.sql.perf.tpcds.Tables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.{ExecutionMode, Benchmark}


object TPCDSBenchmark {

  /*
   * Options:
   *   -gendata generate the database using dsdgen
   *   -sf scale factor
   *   -dsdgen path to the dsdgen tools directory
   *   -iter number of iterations to run
   *   -location where is (generated) the database, might be huge
   *   -cache cache tables
   *   -filter execute a single query
   *   -temp use temporary tables
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
    var cache_tables = false
    var filter = ""
    var temp = true

    options.foreach {
      case ("location", v) => location = v
      case ("sf", v) => scalef = v.toInt
      case ("gendata", v) => gendata = v.toBoolean
      case ("dsdgen", v) => dsdgen = v
      case ("iter", v) => iter = v.toInt
      case ("cache", v) => cache_tables = v.toBoolean
      case ("filter", v) => filter = v
      case ("temp", v) => temp = v.toBoolean
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }

    println(s"sf=$scalef gendata=$gendata iter=$iter cache=$cache_tables filter=$filter temp=$temp")

    val conf = new SparkConf().setAppName("TPCDS Benchmark")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    val sc = new SparkContext(conf)
    //val sqlContext = new HiveContext(sc)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    sqlContext.setConf("spark.sql.perf.results", "results")

    val tables = new Tables(sqlContext, dsdgen, scalef)

    if (gendata) {
      println("Generating TPC-DS data.")
      tables.genData(location = location,
                     format = "parquet",
                     overwrite = true,
                     partitionTables = false,
                     useDoubleForDecimal = false,
                     clusterByPartitionColumns = false,
                     filterOutNullPartitionValues = true,
                     tableFilter = "")
    }

    println(s"Create tables.")
    if (!temp)
      tables.createExternalTables(location, "parquet", "TPC_DS", true)
    else
      tables.createTemporaryTables(location, "parquet")

    val tpcds = new TPCDS()

    println(s"Run experiment.")
    val queries = tpcds.sqlDialectRunnable.filter(_.name contains filter)
    import ExecutionMode._
    /*
    var queries = Seq(tpcds.Query("SELECT count(ss_item_sk)", "SELECT count(*) from store_sales", description = "Custom query", executionMode = CollectResults),
                      tpcds.Query("SELECT *", "SELECT ss_item_sk from store_sales", description = "Custom query", executionMode = CollectResults)
                    )
    */


    queries.foreach { q =>
      println(s"Query: ${q.name}")
      for (i <- 1 to iter) {
        var failed = false
        val start = System.currentTimeMillis()
        val df = sqlContext.sql(q.sqlText.get)
        if (cache_tables)
          df.cache
        try {
          df.collect()
        } catch {
          case e: Exception =>
            println("Failed to run: " + e)
            failed = true
        }
        if (!failed) {
          println(s"   [${q.name} - run ${i}] ${System.currentTimeMillis() - start} ms")
        }
      }
      println("------------------------------------------------------------------")
    }

    /*
    val experiment = tpcds.runExperiment(queries, iterations = iter)

    experiment.waitForFinish(3600*10)

    experiment.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime") as 'minTimeMs,
          max($"executionTime") as 'maxTimeMs,
          avg($"executionTime") as 'avgTimeMs,
          stddev($"executionTime") as 'stdDev)
        .orderBy("name")
        .show(truncate = false)
    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")
    */

    sc.stop()
  }
}
