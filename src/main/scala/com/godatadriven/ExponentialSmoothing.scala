package com.godatadriven

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


import java.util.concurrent.TimeUnit._

import scala.util.Random
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.util.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.util.Collector
import java.beans.Transient
import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object ExponentialSmoothing {

  case class SensorReading(sensorId: Int, measurement: Double, eventTime: Long)

  class SensorSourceFunction(sensorId: Int, baseValue: Double, sigma: Int) extends SourceFunction[SensorReading]() {
    var measurement = baseValue
    @Transient lazy val rand = new Random()

    var isRunning: Boolean = true

    override def run(ctx: SourceContext[SensorReading]) =
      while (isRunning) {
        val rnd = Random.nextGaussian

        measurement = measurement + rnd * sigma

        Thread.sleep(Random.nextInt(200))

        ctx.collect(SensorReading(sensorId, measurement, System.currentTimeMillis))
      }


    override def cancel(): Unit = isRunning = false
  }

  class SensorWatermark extends AssignerWithPeriodicWatermarks[SensorReading] {

    val maxTimeLag = 5000L; // 5 seconds

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
      element.eventTime
    }

    override def getCurrentWatermark(): Watermark = {
      // return the watermark as current time minus the maximum time lag
      new Watermark(System.currentTimeMillis() - maxTimeLag)
    }
  }

  def main(args: Array[String]) {

    // set up the execution environment
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    senv.addSource(new SensorSourceFunction(0, 20, 10)).union(
      senv.addSource(new SensorSourceFunction(1, 50, 20)),
      senv.addSource(new SensorSourceFunction(2, 100, 30)),
      senv.addSource(new SensorSourceFunction(3, 42, 22)),
      senv.addSource(new SensorSourceFunction(4, 140, 40))
    ).assignTimestampsAndWatermarks(new SensorWatermark())
      .keyBy(_.sensorId)
      .reduce((a: SensorReading, b: SensorReading) => SensorReading(a.sensorId, ExponentialSmoothing(a.measurement, b.measurement), a.eventTime))
      .print()


    senv.execute("Flink Streaming Algorithms test")
  }


  // https://grisha.org/blog/2016/01/29/triple-exponential-smoothing-forecasting/
  val alpha = .9

  def ExponentialSmoothing(current: Double, last: Double): Double = alpha * current + (1 - alpha) * last

}
