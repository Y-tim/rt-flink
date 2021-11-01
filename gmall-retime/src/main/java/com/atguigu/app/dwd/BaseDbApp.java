package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.MyStringDeserializationSchema;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author Logan
 * @create 2021-07-29 13:52
 */
//程序： mock -》MySQL -》FlinkCDCApp-》kafka（zk）-》BaseDBApp-》kafka/Phoenix(zk.hdfs.hbase)
public class BaseDbApp {
  public static void main(String[] args) throws Exception {
    // 获取执行环境
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
  /*  //开启ck
    env.enableCheckpointing(5000L);
//    同一时间只允许进行一个检查点
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
    env.getCheckpointConfig().setAlignmentTimeout(10000L);
    env.setStateBackend(new FsStateBackend(""));*/
    //读取kafka的ods_base_db
    String topic ="ods_base_db";
    String groupId="BaseDbApp0225";
    DataStreamSource<String> kafakDS = env.addSource(MyKafkaUtil.getFlinkKafkaCounsumer(topic, groupId));

    //TODO 将消费的数据转换成jsonObject
    SingleOutputStreamOperator<JSONObject> jsonobjDS = kafakDS.map((MapFunction<String, JSONObject>) JSONObject::parseObject).filter(jsonObj -> {
      String type = jsonObj.getString("type");
      return !"delete".equals(type);
    });

    //TODO 通过flinkcdc读取配置信息表，封装为广播流
    DebeziumSourceFunction<String> tableProcessSourceDS = MySQLSource.<String>builder()
            .hostname("hadoop102")
            .port(3306)
            .username("root")
            .password("123456")
            .databaseList("gmall-flink0225")
            .tableList("gmall-flink0225.table_process")
            .startupOptions(StartupOptions.initial())
            .deserializer(new MyStringDeserializationSchema())
            .build();
    DataStreamSource<String> tableProcessDs = env.addSource(tableProcessSourceDS);
    MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
    //我们的key是sourcetable+type，然后value就是一行一行的数据，我们封装成的processTable
    BroadcastStream<String> broadcastStream = tableProcessDs.broadcast(mapStateDescriptor);

    //TODO 连接主流和广播流
    BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = jsonobjDS.connect(broadcastStream);
    //定义一个侧输出流，侧输出到hbase
    OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase"){};
  //传入状态描述和侧输出流
    SingleOutputStreamOperator<JSONObject> mainDS = broadcastConnectedStream.process(new TableProcessFunction(mapStateDescriptor, hbaseTag));
    //获取hbase流数据
    DataStream<JSONObject> hbaseDS = mainDS.getSideOutput(hbaseTag);

    //获取kafka数据流以及hbase数据流写入对应的存储框架中
    mainDS.print("Kafka>>>>>>>");
    hbaseDS.print("hbase>>>>>>>");

    hbaseDS.addSink(new DimSinkFunction());
    mainDS.addSink(MyKafkaUtil.getFlinkafkaProducer(new KafkaSerializationSchema<JSONObject>() {
      @Override
      public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
        return new ProducerRecord<>(jsonObject.getString("sinkTable"),
                  jsonObject.getString("data").getBytes()
                );
      }
    }));


//启动任务
    env.execute();

  }
}
