import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Increment, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object StreamRides {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("chicago_transportation_hourly"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamRides")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("nselman-chicago-transportation")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[RideReport]))

    // How to write to an HBase table
    val batchStats = reports.map(rr => {
      val key = (rr.year + ":" + rr.month + ":" + rr.daytype +
        ":" + rr.pickup_community_area + ":" + rr.dropoff_community_area +
        ":" + rr.hour + ":" + rr.ridetype)
      System.out.println("key:", key)

      val maybeRow = table.get(new Get(Bytes.toBytes(key)))
      System.out.println(maybeRow)
      if (maybeRow.isEmpty) {
        System.out.println("entered the if block where it's empty")
        val put = new Put(Bytes.toBytes(key))
        put.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("duration_seconds"), Bytes.toBytes(rr.duration_seconds))
        put.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("miles_tenths"), Bytes.toBytes(rr.miles_tenths))
        put.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("tip_cents"), Bytes.toBytes(rr.tip_cents))
//        put.addColumn(Bytes.toBytes("stats"),
//          Bytes.toBytes("additional_charges"), Bytes.toBytes(rr.tip_cents))
        put.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("trip_total_cents"), Bytes.toBytes(rr.trip_total_cents))
        put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("num_rides"),
          Bytes.toBytes(1.toLong))
        table.put(put)
      }
      else {
        System.out.println("entered else block")
        val increment = new Increment(Bytes.toBytes(key))
        System.out.println("created increment")
        increment.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("duration_seconds"), rr.duration_seconds)
        System.out.println("finished duration finished")
        increment.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("miles_tenths"), rr.miles_tenths)
        System.out.println("miles_tenths finished")
        increment.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("tip_cents"), rr.tip_cents)
        System.out.println("tip_cents fnished")
//        increment.addColumn(Bytes.toBytes("stats"),
//          Bytes.toBytes("additional_charges"), rr.tip_cents)
        increment.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("trip_total_cents"), rr.trip_total_cents)
        System.out.println("trip_total_cents finish")
        increment.addColumn(Bytes.toBytes("stats"),
          Bytes.toBytes("num_rides"), 1)
        System.out.println("num rides finished")
        table.increment(increment)
      }
    })
    System.out.println("finished that block session")
    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
