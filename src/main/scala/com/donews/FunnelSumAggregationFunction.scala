package com.donews

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import java.util

/**
 * @title FunnelSumAggregationFunction
 * @author Niclas
 * @date 2021/3/26 17:37
 * @description
 * 漏斗分析函数 FUNNEL_SUM
 *
 * 语法：
 * funnel_sum(<通过funnel_count计算出来的最长事件匹配链:int>,<共有几个事件:int>)
 *
 * 函数说明：
 * FUNNEL_SUM也是聚合函数，搭配FUNNEL_COUNT使用，对漏斗函数做二次聚合。
 * 对每个最长有序事件进行匹配统计，根据关心的事件个数参数，依次统计完成1次转换、2次转化、3次转化等。
 *
 * 示例：
 * FUNNEL_COUNT聚合的结果如下：
 * +---+------------------------+
 * |uid|max_ordered_match_length|
 * +---+------------------------+
 * |3  |2                       |
 * |1  |3                       |
 * |4  |1                       |
 * |2  |0                       |
 * +---+------------------------+
 * 使用该函数再次聚合之后可得到如下结果：
 * +---------------+
 * |conversion_link|
 * +---------------+
 * |[3, 2, 1, 0]   |
 * +---------------+
 * -- 用户1完成了连续3个事件，用户3则完成2个，用户4完成1个，用户2没有，假如关心的事件数为4，那么：
 * -- funnel_sum(max_ordered_match_length, 4) = [3, 2, 1, 0] ，
 * -- 有3个用户（分别是1，3，2）完成的事件数大于等于1；
 * -- 有2个用户（分别是1，3）完成的事件数大于等于2；
 * -- 有1个用户（就是1这个用户）完成的事件数大于等3；
 * -- 有0个用户完成的事件数大于等4；
 * -- 所以函数的结果从1开始分别统计每个事件完成的人数[3,2,1,0]
 *
 * 完整的sql示例：
 * SELECT funnel_sum(max_ordered_match_length, 4) AS conversion_link
 * FROM (
 * SELECT uid,funnel_count(event_time, 10, event_id, '1,3,6,7') AS max_ordered_match_length
 * FROM test_tb
 * GROUP BY uid ) AS tmp
 *
 * 示例代码见：com.donews.SparkFunnelTest
 */
class FunnelSumAggregationFunction extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(
    // 传入参数定义,[用户状态,事件个数]
    Array(
      StructField("userState", DataTypes.IntegerType), // 每个用户的状态
      StructField("eventCount", DataTypes.IntegerType) // 查询事件的个数
    )
  )

  override def bufferSchema: StructType = StructType(
    // 缓存结果数据,
    // 存储方式为[事件个数,完成1个事件的用户数,完成2个事件的用户数,...,完成eventCount个事件的用户数]
    Array(
      StructField("conversionCount",
        DataTypes.createArrayType(
          DataTypes.IntegerType
        ))
    ))

  override def dataType: DataType = DataTypes.createArrayType(DataTypes.IntegerType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new util.ArrayList[Int]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 用户状态
    val userState = input.getInt(0)
    // 事件个数
    val eventCount = input.getInt(1)
    var eventAndUserCount = buffer.getList[Int](0)
    // 将事件个数和用户总数存入,如果为空列表,则初始化列表
    if (eventAndUserCount.isEmpty && eventCount > 0) {
      val arrayList = new util.ArrayList[Int](eventAndUserCount)
      arrayList.add(eventCount)
      for (_ <- 1 to eventCount) {
        arrayList.add(0)
      }
      eventAndUserCount = arrayList
    }
    // 计算用户数
    for (index <- 1 to eventCount if eventCount > 0 && index <= userState) {
      val userCount = eventAndUserCount.get(index) + 1
      eventAndUserCount.set(index, userCount)
    }
    buffer(0) = eventAndUserCount
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var eventAndUserCount1 = buffer1.getList[Int](0)
    val eventAndUserCount2 = buffer2.getList[Int](0)
    // 合并多节点结果数据
    if (!eventAndUserCount2.isEmpty) {
      val eventCount = eventAndUserCount2.get(0)
      // 第一次计算时初始化buffer1(0)的值
      if (eventAndUserCount1.isEmpty && eventCount > 0) {
        val arrayList = new util.ArrayList[Int](eventCount)
        for (_ <- 0 until eventCount) {
          arrayList.add(0)
        }
        eventAndUserCount1 = arrayList
      }
      // 对数据求和
      for (i <- 0 until eventCount if eventCount > 0) {
        // 解决java.lang.UnsupportedOperationException的异常问题
        val arrayList = new util.ArrayList[Int](eventAndUserCount1)
        val mergeCount = arrayList.get(i) + eventAndUserCount2.get(i + 1)
        arrayList.set(i, mergeCount)
        eventAndUserCount1 = arrayList
      }
    }
    buffer1(0) = eventAndUserCount1
  }

  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}
