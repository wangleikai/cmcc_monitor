package cn.sheep.app

import java.text.SimpleDateFormat

import cn.sheep.utils.{ConfUtil, Jpools}
import com.alibaba.fastjson.{JSON, JSONArray, JSONException, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}




/**
  * @author WangLeiKai
  *         2018/10/16  9:15
  */
object AppMain {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("AppMain").setMaster("local[*]")

    //序列化方式
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //压缩RDD
    conf.set("spark.rdd.compress","true")
    //batchSize = 分区的数量 * 采样时间 * maxRatePerPartition
    conf.set("spark.streaming.kafka.maxRatePerPartition","100")
    //是否优雅的退出
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(2))


    val stream =
      KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](ConfUtil.topic, ConfUtil.kafkaParams)
      )

    stream.foreachRDD(rdd=>{
      //取得所有充值通知日志
      val baseData: RDD[JSONObject] = rdd.map(cr => JSON.parseObject(cr.value()))
        .filter(obj=> obj.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq")).cache()

      /**
        * 获取到充值成功的订单笔数
        * 回忆：
        *   wordcount flatMap->map->reduceByKey
        *   hadoop spark hadoop
        */
      val totalSucc =  baseData.map(obj=>{
        val reqId = obj.getString("requestId")
        //获取日期
        val day = reqId.substring(0, 8)
        //取出该条充值是否成功的标志
        val result = obj.getString("bussinessRst")
        val flag = if(result.equals("0000")) 1 else 0
        (day, flag)
      }).reduceByKey(_+_)

      /**
        * 获取充值成功的订单金额
        */
      val totalMoney = baseData.map(obj=>{
        val reqId = obj.getString("requestId")
        //获取日期
        val day = reqId.substring(0, 8)
        //取出该条充值是否成功的标志
        val result = obj.getString("bussinessRst")
        val fee = if(result.equals("0000")) obj.getString("chargefee").toDouble  else 0
        (day, fee)
      }).reduceByKey(_+_)

      //总订单量
      val total = baseData.count()

      /**
        * 获取充值成功的充值时长
        */
      val totalTime = baseData.map(obj=>{
        var reqId = obj.getString("requestId")
        //获取日期
        val day = reqId.substring(0, 8)

        //取出该条充值是否成功的标志
        val result = obj.getString("bussinessRst")
        //时 间 格 式 为: yyyyMMddHHmissSSS(( 年月日时分秒毫秒)
        val endTime = obj.getString("receiveNotifyTime")
        val startTime =  reqId.substring(0, 17)

        val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        val cost =  if(result.equals("0000")) format.parse(endTime).getTime - format.parse(startTime).getTime else 0

        (day, cost)
      }).reduceByKey(_+_)

      //将充值成功的订单数写入到Redis
      totalSucc.foreachPartition(itr =>{
        val jedis = Jpools.getJedis
        itr.foreach(tp=>{
          jedis.incrBy("CMCC-"+tp._1, tp._2)
        })
      })
    })

    ssc.start()

    ssc.awaitTermination()

  }

}
