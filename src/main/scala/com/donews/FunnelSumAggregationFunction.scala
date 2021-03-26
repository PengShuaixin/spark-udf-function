package com.donews

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
 * @title FunnelSumAggregationFunction
 * @author Niclas
 * @date 2021/3/26 17:37
 * @description
 */
class FunnelSumAggregationFunction extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(
    Array(
      StructField("userState", DataTypes.IntegerType), // 每个用户的状态
      StructField("eventCount", DataTypes.IntegerType) // 查询事件的个数
    )
  )

  override def bufferSchema: StructType = StructType(
    Array(
      StructField("conversionCount",
        DataTypes.createMapType(
          DataTypes.IntegerType, DataTypes.IntegerType
        ))
    ))

  override def dataType: DataType = DataTypes.createArrayType(DataTypes.IntegerType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val userState = input.getInt(0)
    val eventCount = input.getInt(1)
    var conversionCount: collection.Map[Int, Int] = buffer.getMap[Int, Int](0)
    for (i <- 0 until eventCount) {
      if (userState > i) {
        val count = conversionCount.getOrElse(i, 0) + 1
        conversionCount = conversionCount + (i -> count)
      }
    }
    buffer(0) = conversionCount
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

  }

  override def evaluate(buffer: Row): Any = {
    Array(0)
  }
}
