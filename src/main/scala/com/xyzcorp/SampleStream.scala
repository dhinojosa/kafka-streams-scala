package com.xyzcorp

import java.time.Duration
import java.util.Properties

import org.apache.kafka.common.config.Config
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.SessionWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.{timeWindowedSerde => _, _}
import org.apache.kafka.streams.scala.StreamsBuilder

class SampleStream {
  def createTopology(conf: Config, properties: Properties): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder()
    builder
      .stream[String, String]("streams-plaintext-input")
      .groupBy((_, word) => word)
      .windowedBy(SessionWindows.`with`(Duration.ofMillis(60 * 1000)))
      .count()
      .toStream
      .to("streams-pipe-output")

    builder.build()
  }
}
