package cn.sheep.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * @author WangLeiKai
  *         2018/10/17  15:43
  */
object KpiTools {
  /**
    * 获取实时的成功订单数和成交金额
    * @param baseData
    */
  def kpi_quality(baseData: RDD[(String, String, String, List[Double], String)]): Unit = {
    baseData.map(tp => ((tp._1, tp._2, tp._3), List(tp._4(1), tp._4(2)))).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    })
      .foreachPartition(partition => {
        val jedis = Jpools.getJedis
        partition.foreach(tp => {

          //订单数
          jedis.hincrBy("D-" + tp._1._1, "Province" + tp._1._2 + tp._1._3, tp._2(0).toLong)
          //每分钟的成交金额
          jedis.hincrBy("D-" + tp._1._1, "saalry" + tp._1._2 + tp._1._3, tp._2(1).toLong)
          // key的有效期
          jedis.expire("D-" + tp._1._1, 48 * 60 * 60)
        })
        jedis.close()
      })
  }

  /**
    * 查看每天各个省份的失败的数量
    * @param baseData
    * @param pcode2pname
    */
  def Kpi_faliure(baseData: RDD[(String, String, String, List[Double], String)], pcode2pname: Broadcast[Map[String, AnyRef]]): Unit = {
    //  全国业务失败量
    // (日期, 小时, Kpi(订单，成功订单，订单金额，订单时长))
    baseData.map(tp => ((tp._1, tp._5), tp._4(1))).reduceByKey(_ + _)
      .foreachPartition(partition => {
        val jedis = Jpools.getJedis
        partition.foreach(tp => {

          "Province" + tp._1._2
          jedis.hincrBy("C-" + tp._1._1, pcode2pname.value.getOrElse(tp._1._2, tp._1._2).toString, tp._2.toLong)
          // key的有效期
          jedis.expire("C-" + tp._1._1, 48 * 60 * 60)
        })
        jedis.close()
      })
  }

  /**
    *查看每天每小时的成交量和总的订单数
    * @param baseData
    */
  def Kpi_shishi(baseData: RDD[(String, String, String, List[Double], String)]): Unit = {
    //实时充值业务
    baseData.map(tp => ((tp._1, tp._2), List(tp._4(0), tp._4(1)))).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    })
      .foreachPartition(partition => {
        val jedis = Jpools.getJedis
        partition.foreach(tp => {
          jedis.hincrBy("B-" + tp._1._1, "total" + tp._1._2, tp._2(0).toLong)
          jedis.hincrBy("B-" + tp._1._1, "succ" + tp._1._2, tp._2(1).toLong)
          // key的有效期
          jedis.expire("B-" + tp._1._1, 48 * 60 * 60)
        })
        jedis.close()
      })
  }

  /**
    * 查看总的金额 成交量
    * @param baseData
    */
  def Kpi_jichu(baseData: RDD[(String, String, String, List[Double], String)]): Unit = {
    baseData.map(tp => (tp._1, tp._4)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    })
      .foreachPartition(partition => {
        val jedis = Jpools.getJedis
        partition.foreach(tp => {
          jedis.hincrBy("A-" + tp._1, "total", tp._2(0).toLong)
          jedis.hincrBy("A-" + tp._1, "succ", tp._2(1).toLong)
          jedis.hincrByFloat("A-" + tp._1, "money", tp._2(2))
          jedis.hincrBy("A-" + tp._1, "cost", tp._2(3).toLong)
          // key的有效期
          jedis.expire("A-" + tp._1, 48 * 60 * 60)
        })
        jedis.close()
      })
  }
}
