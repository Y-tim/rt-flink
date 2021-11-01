package com.atguigu;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;

import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author Logan
 * @create 2021-07-27 18:41
 */
public class FlinkCDC {
  public static void main(String[] args) throws Exception {
    // 创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    //设置用户名
    System.setProperty("HADOOP_USER_NAME","atguigu");
    //开启ck
    env.enableCheckpointing(5000L);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setAlignmentTimeout(10000L);
    //设置状态存储位置
    env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ck"));
    //通过flinkcdc构建source
    DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
            .hostname("hadoop102")
            .port(3306)
            .username("root")
            .password("123456")
            .databaseList("gmall-flink0225")
            .tableList("gmall-flink0225.base_trademark")
            .startupOptions(StartupOptions.initial())
            .deserializer(new StringDebeziumDeserializationSchema())
            .build();
    DataStreamSource<String> result = env.addSource(sourceFunction);
    result.print();
    env.execute();

  }
}
