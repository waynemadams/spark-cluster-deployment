package com.adamsresearch.spark.deployment.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.io.Source

/**
 * Created by wma on 2/28/15.
 */
object SparkDeploymentDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    println("SparkConf:  " + conf.toDebugString)
    val  sc = new SparkContext(conf)

    val filename = "usgs_09085100_dischargerate.tsv"
    val lines = Source.fromInputStream(this.getClass().getClassLoader().getResourceAsStream(filename)).getLines()
    // lines containing actual gauge data start with "USGS":
    val dataLines = lines.filter(line => line.startsWith("USGS"))
    val dataFileRdd = sc.parallelize(dataLines.toList)
    val dischargeRateRdd = dataFileRdd.map(line => {
      val fields = line.split("\\\t")
      fields(3).toFloat
    })

    // gets stats on discharge rate:
    val startTime = System.nanoTime()
    val statCounter = dischargeRateRdd.stats()
    println("mean discharge rate: " + statCounter.mean)
    println("max discharge rate: " + statCounter.max)
    println("min discharge rate: " + statCounter.min)
    println("std dev: " + statCounter.stdev)
    println("var: " + statCounter.variance)
    val stopTime = System.nanoTime()
    println("total time: " + (stopTime - startTime)/1000000L + " ms")
  }
}
