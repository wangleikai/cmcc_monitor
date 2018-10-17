package cn.sheep.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer


/**
  * @author WangLeiKai
  *         2018/10/16  9:32
  */
object ConfUtil {
  //解析配置文件
  //conf json properties

  private lazy val config: Config = ConfigFactory.load()

  val topic = config.getString("kafka.topic").split(",")

  val groupId: String = config.getString("kafka.group.id")


  val redisHost: String = config.getString("redis.host")
  val selectDBIndex = config.getInt("redis.db.index")

  val broker: String = config.getString("kafka.broker.list")

  import scala.collection.JavaConversions._

  val pcode2pname = config.getObject("pcode2pname").unwrapped().toMap
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> broker,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupId,
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false"
  )

}
