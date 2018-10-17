package cn.sheep.app

/**
  * 中国移动运营实时监控平台
  *
  * @author WangLeiKai
  *         2018/10/17  8:42
  */

import cn.sheep.utils.{CaculateTools, ConfUtil, KpiTools}
import com.alibaba.fastjson.{JSON}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object AppMain2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("中国移动运营实时监控平台-Monitor")
    //如果在集群上运行的话，需要去掉：sparkConf.setMaster("local[*]")
    sparkConf.setMaster("local[*]")
    //默认采用org.apache.spark.serializer.JavaSerializer
    //这是最基本的优化
    //将rdd以序列化格式来保存以减少内存的占用
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //rdd压缩
    sparkConf.set("spark.rdd.compress", "true")
    //batchSize = partitionNum * 分区数量 * 采样时间
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    //优雅的停止
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    /**
      * 广播省份code和name对应信息
      */
    val pcode2pname: Broadcast[Map[String, AnyRef]] = ssc.sparkContext.broadcast(ConfUtil.pcode2pname)

    /** 获取kafka的数据
      * LocationStrategies：位置策略，如果kafka的broker节点跟Executor在同一台机器上给一种策略，不在一台机器上给另外一种策略
      * 设定策略后会以最优的策略进行获取数据
      * 一般在企业中kafka节点跟Executor不会放到一台机器的，原因是kakfa是消息存储的，Executor用来做消息的计算，
      * 因此计算与存储分开，存储对磁盘要求高，计算对内存、CPU要求高
      * 如果Executor节点跟Broker节点在一起的话使用PreferBrokers策略，如果不在一起的话使用PreferConsistent策略
      * 使用PreferConsistent策略的话，将来在kafka中拉取了数据以后尽量将数据分散到所有的Executor上 */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](ConfUtil.topic, ConfUtil.kafkaParams))

    /**
      * 数据处理
      */
    stream.foreachRDD(rdd => {
      val baseData = rdd
        // ConsumerRecord => JSONObject
        .map(cr => JSON.parseObject(cr.value()))
        // 过滤出充值通知日志
        .filter(obj => obj.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
        .map(obj => {

          // 判断该条日志是否是充值成功的日志
          val result = obj.getString("bussinessRst")
          //充值的金额
          val fee = obj.getDouble("chargefee")
          //获取数据对应的省份的code
          val provinceCode = obj.getString("provinceCode")
          // 充值发起时间和结束时间
          val requestId = obj.getString("requestId")
          // 数据当前日期，小时，分钟
          val day = requestId.substring(0, 8)
          val hour = requestId.substring(8, 10)
          val minute = requestId.substring(10, 12)
          val receiveTime = obj.getString("receiveNotifyTime")

          //充值花费的时间
          val costTime = CaculateTools.caculateTime(requestId, receiveTime)
          val succAndFeeAndTime: (Double, Double, Double) = if (result.equals("0000")) (1, fee, costTime) else (0, 0, 0)

          // (日期, 小时,分钟， Kpi(订单，成功订单，订单金额，订单时长),省份)
          (day, hour, minute, List[Double](1, succAndFeeAndTime._1, succAndFeeAndTime._2, succAndFeeAndTime._3), provinceCode)
        }).cache()

      //求订单量 充值金额 花费时间

      KpiTools.Kpi_jichu(baseData)
      KpiTools.Kpi_shishi(baseData)

      KpiTools.Kpi_faliure(baseData, pcode2pname: Broadcast[Map[String, AnyRef]])

      //每分钟实时充值情况分布
      // (日期, 小时,分钟， Kpi(订单，成功订单，订单金额，订单时长),省份)
      KpiTools.kpi_quality(baseData)
    })

    //启动sparkstreaming
    ssc.start()
    //优雅的停止
    ssc.awaitTermination()
  }


}
