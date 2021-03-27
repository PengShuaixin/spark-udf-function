package com.donews

import org.apache.spark.sql.SparkSession

/**
 * @title SparkFunnelCountTest
 * @author Niclas
 * @date 2021/3/26 9:48
 * @description funnel函数示例代码
 */
object SparkFunnelTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkFunnelTest")
      .master("local[*]")
      .getOrCreate()

    // 读取数据源，创建临时表
    val testDF = spark.read.option("header", "true").csv("funnel-test-data.csv")
    testDF.createOrReplaceTempView("test_tb")

    // 注册funnel_count函数
    val funnelCount = new FunnelCountAggregationFunction()
    spark.udf.register("funnel_count", funnelCount)

    // 注册funnel_sum函数
    val funnelSum = new FunnelSumAggregationFunction()
    spark.udf.register("funnel_sum", funnelSum)

    // 测试funnel函数
    val funnelCountDF = spark.sql(
      """
        |SELECT funnel_sum(max_ordered_match_length, 4) AS conversion_link
        |FROM (
        |SELECT uid,funnel_count(event_time, 10, event_id, '1,3,6,7') AS max_ordered_match_length
        |FROM test_tb
        |GROUP BY uid ) AS tmp
        |""".stripMargin)
    funnelCountDF.show(false)

    spark.stop()
  }
}
