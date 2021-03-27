package com.donews

import java.util
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @title FunnelCountAggregationFunction
 * @author Niclas
 * @date 2021/3/25 14:15
 * @description
 * 漏斗分析函数 FUNNEL_COUNT
 *
 * 语法：
 * funnel_count(<事件发送时间戳(毫秒):bigint>, <事件窗口毫秒宽度:bigint>, <事件id:int/varchar>, <事件id列表:varchar>)
 *
 * - 过滤所有满足事件窗口宽度内的数据。
 * - 将过滤后的事件按时间戳排序，例如t1发送e1、t2发送e2、t3发送e1、t4发送e3。
 * - 对比事件列表，从第一个事件依次做“最长、有序”匹配，返回匹配的最大长度。一旦匹配失败，结束整个匹配。
 * 例如关心事件为e1，e2，e3，而用户数据为e1，e2，e4，最终匹配到e1，e2，函数返回值为2。
 * 关心事件为e1，e2，e3，而用户数据为e2，e1，e3，最终匹配到e1，函数返回值为1。
 * 关心事件为e1，e2，e3，而用户数据为e4，e3，最终没有匹配到事件，函数返回值为0。
 *
 *
 * 示例数据：
 *
 * uid,event_time,event_id
 * 1,2,1
 * 3,4,1
 * 1,4,3
 * 1,5,3
 * 2,5,3
 * 3,6,3
 * 4,1,1
 * 4,11,3
 * 1,7,6
 *
 * 示例SQL:
 * SELECT uid,
 * funnel_count(event_time, 10, event_id, '1,3,6,7') AS max_ordered_match_length
 * FROM test_tb
 * GROUP BY uid
 *
 * 示例返回值：
 *
 * +---+------------------------+
 * |uid|max_ordered_match_length|
 * +---+------------------------+
 * |3  |2                       |
 * |1  |3                       |
 * |4  |1                       |
 * |2  |0                       |
 * +---+------------------------+
 *
 * -- 用户1，在时间点2触发了1号事件，在时间点4和5触发了3号事件，在时间点7触发了6号事件，与关心事件列表(1,3,6,7)相比，匹配了(1,3,6) 三个事件，所以函数返回值为3。
 * -- 用户3，在时间点4触发了1号事件，在时间点6触发了3号事件，与关心事件列表(1,3,6,7)相比，匹配了(1,3）两个事件，所以函数返回值为2。
 * -- 用户2，在时间点5触发了3号事件，与关心事件列表(1,3,6,7)相比，匹配了0个事件（因为第一个事件是1），所以函数返回值为0。
 * -- 用户4，在时间点1触发了1号事件，在时间点11触发了3号事件（这个事件超过窗口10，无效），与关心事件列表(1,3,6,7)相比，匹配(1） 一个事件，所以函数返回值为1。​
 *
 * 示例代码见：com.donews.SparkFunnelTest
 *
 */
class FunnelCountAggregationFunction extends UserDefinedAggregateFunction {

  // 输入数据Schema信息
  override def inputSchema: StructType = StructType(
    Array(
      StructField("timestamp", DataTypes.LongType), // 当前事件的时间戳
      StructField("windows", DataTypes.LongType), // 当前查询的时间窗口大小
      StructField("event", DataTypes.StringType), // 当前事件的名称, A还是B或者C
      StructField("funnel", DataTypes.StringType) // 当前查询的全部事件, 逗号分隔
    )
  )

  // 缓存数据Schema信息
  override def bufferSchema: StructType = StructType(
    Array(
      // 存储漏斗中事件对应的下标
      StructField("funnelEventIndex",
        DataTypes.createMapType(
          DataTypes.StringType,
          DataTypes.ByteType)),
      // 存储时间窗口大小、漏斗长度
      StructField("windowAndFunnelLen",
        DataTypes.createArrayType(
          DataTypes.IntegerType)),
      // 存储事件时间戳、事件下标的映射
      StructField("timestampAndEvent",
        DataTypes.createMapType(
          DataTypes.IntegerType,
          DataTypes.ByteType)),
      // 存储计算结果数据
      StructField("maxEventIndex",
        DataTypes.IntegerType)
    )
  )

  override def dataType: DataType = DataTypes.IntegerType

  override def deterministic: Boolean = true

  // 计算之前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(2) = new mutable.HashMap[Int, Byte]()
    // 默认值0
    buffer(3) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 根据,切分漏斗事件参数
    val funnelEvents: Array[String] = input.getString(3).split(",")
    // 漏斗中的事件和事件对应的下标组成Map
    val funnelEventDict: Map[String, Byte] = (for (i <- funnelEvents.indices) yield (funnelEvents(i), i.toByte)).toMap
    // 如果缓存为空，将输入数据的存入缓存
    if (buffer(0) == null) {
      buffer(0) = funnelEventDict
    }
    // 将漏斗时间窗口大小和漏斗的长度存入缓存
    if (buffer(1) == null) {
      val funnelLen = funnelEvents.length
      val window = input.getLong(1).toInt
      buffer(1) = Array(funnelLen, window)
    }
    // 将时间戳和事件载入缓存
    val timestamp = input.getLong(0).toInt
    val event = input.getString(2)
    val eventIndex = funnelEventDict.getOrElse(event, null)
    if (eventIndex != null) {
      buffer(2) = buffer.getMap[Int, Byte](2) + (timestamp -> eventIndex)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 事件发生的时间和事件下标
    val timestampAndIndex: collection.Map[Int, Byte] = buffer2.getMap[Int, Byte](2)
    // 判断是否存在漏斗中的第一个event,不存在直接返回0,不继续执行
    if (!timestampAndIndex.values.toList.contains(0)) {
      return
    }
    // 构造时间戳列表并排序
    val timeArray = timestampAndIndex.keys.toList.sorted
    // 漏斗的长度和时间窗口
    val funnelLenAndWindow: util.List[Int] = buffer2.getList[Int](1)
    // 获取中间变量
    val funnelLen = funnelLenAndWindow.get(0)
    val window = funnelLenAndWindow.get(1)

    // 遍历时间戳数据, 也就是遍历有序事件, 并构造结果
    var maxEventIndex = 0;
    val eventTemps: ListBuffer[Array[Int]] = ListBuffer[Array[Int]]()
    for (timestamp <- timeArray) {
      // 事件有序进入
      val eventIndex = timestampAndIndex(timestamp)
      if (eventIndex == 0) {
        val flag = Array(timestamp, eventIndex)
        eventTemps += flag
      } else {
        // 更新临时对象: 从后往前, 并根据条件适当跳出
        var forFlag = true
        for (eventTemp <- eventTemps.reverse if forFlag) {
          if ((timestamp - eventTemp(0)) >= window) {
            // 当前事件的时间戳减去startEvent[0]超过时间窗口不合法, 跳出
            forFlag = false
          } else if (eventIndex == (eventTemp(1) + 1)) {
            // 当前事件为下一个事件, 更新数据并跳出
            eventTemp(1) = eventIndex
            if (maxEventIndex < eventIndex) {
              maxEventIndex = eventIndex
            }
            forFlag = false
          }
          // 漏斗流程结束, 提前退出
          if ((maxEventIndex + 1) == funnelLen) {
            forFlag = false
          }
        }
      }
    }
    buffer1(3) = maxEventIndex + 1
  }

  override def evaluate(buffer: Row): Any = {
    // 完成事件的个数
    buffer(3)
  }
}
