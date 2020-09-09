package top.zhangchao.hotitem

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import top.zhangchao.bean.{UserBehavior}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/5 4:29 下午
 */
object HotItemsWithTableApi {
  def main(args: Array[String]): Unit = {
    // create Execution Env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // set time characteristic as event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序, 我们配置全局的并发为 1, 这里改变并发对结果正确性没有影响
    env.setParallelism(1)

    val fileInputPath = "/Users/amos/Learning/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv"

    val userBehaviorStream: DataStream[UserBehavior] = env
      .readTextFile(fileInputPath)
      .map(line => {
        val lineArr: Array[String] = line.split(",")
        UserBehavior(lineArr(0).toLong, lineArr(1).toLong, lineArr(2).toInt, lineArr(3), lineArr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) // 指定时间戳和 watermark

    // 表环境的创建
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 将 Data
    val dataTable: Table = tableEnv.fromDataStream(userBehaviorStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    // 基于 Table Api 进行窗口聚合
    val aggTable: Table = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hour every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'itemId.count as 'cnt, 'sw.end as 'windowEnd)

//    aggTable.toAppendStream[Row].print("agg")

    // 排序输出
    tableEnv.createTemporaryView("agg", aggTable, 'itemId, 'cnt, 'windowEnd)
    val resultTable: Table = tableEnv.sqlQuery(// 先判断 where 条件, 所以要用子查询
      """
        |select *
        |from (
        |select *, row_number() over(partition by windowEnd order by cnt desc) as row_num
        |from agg
        |) t
        |where row_num <= 5
        |""".stripMargin
    )

    resultTable.toRetractStream[Row].print()

    env.execute()
  }

}
