package com.atguigu.app.dws;


import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
  public static void main(String[] args) throws Exception {
    // TODO 执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    // 开启ck
/*    env.enableCheckpointing(5000L);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
    env.getCheckpointConfig().setAlignmentTimeout(10000L);
    env.setStateBackend(new FsStateBackend(""));*/
    //TODO 创建表执行环境
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // TODO 从kafka dwm_order_wide获取数据
    String groupId = "province_stats";
    String orderWideTopic = "dwm_order_wide";
    tableEnv.executeSql("CREATE TABLE order_wide (" +
            "  `province_id` BIGINT, " +
            "  `province_name` STRING, " +
            "  `province_area_code` STRING, " +
            "  `province_iso_code` STRING, " +
            "  `province_3166_2_code` STRING, " +
            "  `order_id` BIGINT, " +
            "  `total_amount` DOUBLE, " +
            "  `create_time` STRING, " +
            "  `rt` AS TO_TIMESTAMP(create_time), " +
            "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
            ")" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId));
    // TODO 分组开窗聚合
    Table resultTable = tableEnv.sqlQuery("select  " +
            "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,  " +
            "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,  " +
            "    province_id,  " +
            "    province_name,  " +
            "    province_area_code,  " +
            "    province_iso_code,  " +
            "    province_3166_2_code,  " +
            "    sum(total_amount) order_amount,  " +
            "    count(distinct order_id) order_count,  " +
            "    UNIX_TIMESTAMP() AS ts " +
            "from order_wide  " +
            "group by  " +
            "    province_id,  " +
            "    province_name,  " +
            "    province_area_code,  " +
            "    province_iso_code,  " +
            "    province_3166_2_code,  " +
            "    TUMBLE(rt, INTERVAL '10' SECOND)");
    //测试打印
//        tableEnv.executeSql("select * from order_wide").print();
    // TODO 将动态表转换为数据流
    DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(resultTable, ProvinceStats.class);
    // TODO 将数据写入clickhouse
    provinceStatsDataStream.print(">>>>>>>>>>>>>");
    provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats_210225 values(?,?,?,?,?,?,?,?,?,?)"));
    // TODO 启动任务
    env.execute();

  }
}
