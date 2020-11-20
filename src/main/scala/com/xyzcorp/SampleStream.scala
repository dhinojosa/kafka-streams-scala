import java.time.Duration
import java.util.Properties

import org.apache.kafka.common.config.Config
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{SessionWindows, Windowed}
import org.apache.kafka.streams.scala.Serdes.{timeWindowedSerde => _, _}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.scala.{ByteArraySessionStore, StreamsBuilder}

class SampleStream {
  def createTopology(conf: Config, properties: Properties): Topology = {

    implicit val produced: Produced[Windowed[String], Long] =
      Produced.`with`[Windowed[String], Long]

    implicit val grouped: Grouped[String, String] =
      Grouped.`with`[String, String]

    implicit val consumed: Consumed[String, String] =
      Consumed.`with`[String, String]

    implicit val materialized: Materialized[String, Long, ByteArraySessionStore]
        = Materialized.`with`[String, Long, ByteArraySessionStore]

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
