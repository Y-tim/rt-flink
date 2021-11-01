package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;

import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author Logan
 * @create 2021-07-27 20:28
 */
public class FlinkCDCByMyDeser {
  public static void main(String[] args) throws Exception {

    // 创建执行环境
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    // 通过flinkcdc构建数据源

    DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
            .hostname("hadoop102")
            .port(3306)
            .username("root")
            .password("123456")
            .databaseList("gmall-flink0225")
            .tableList("gmall-flink0225.base_trademark")
            .startupOptions(StartupOptions.initial())
            .deserializer(new MyStringDeserializationSchema())
            .build();
    DataStreamSource<String> result = env.addSource(sourceFunction);
    result.print();
    env.execute();
  }

  public static class MyStringDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
        throws Exception {
      // 构建结果对象
      JSONObject result = new JSONObject();
      // 获取数据名称和表名称
      String topic = sourceRecord.topic();
      String[] split = topic.split("\\.");
      String db = split[1];
      String tb = split[2];
      // 获取数据after
      Struct value = (Struct) sourceRecord.value();
      Struct after = (Struct) value.get("after");
      JSONObject data = new JSONObject();
      if (after != null) { // delete数据，则after为null
        Schema schema = after.schema();
        List<Field> fieldList = schema.fields();
        for (int i = 0; i < fieldList.size(); i++) {
          Field field = fieldList.get(i);
          Object fieldValue = after.get(field);
          data.put(field.name(), fieldValue);
        }
      }
      // 获取数据befor
      Struct before = (Struct) value.get("before");
      JSONObject beforeData = new JSONObject();
      if (before != null) { // delete数据，则after为null
        Schema schema = before.schema();
        List<Field> fieldList = schema.fields();
        for (int i = 0; i < fieldList.size(); i++) {
          Field field = fieldList.get(i);
          Object fieldValue = before.get(field);
          data.put(field.name(), fieldValue);
        }
      }

      // 获取操作类型
      Envelope.Operation operation = Envelope.operationFor(sourceRecord);
      String type = operation.toString().toLowerCase();
      if ("create".equals(type)) {
        type = "insert";
      }
      result.put("database", db);
      result.put("tableName", tb);
      result.put("data",data);
      result.put("before",beforeData);
      result.put("type",type);
      result.put("ts",System.currentTimeMillis());
      // 输出封装好的数据
      collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
      return BasicTypeInfo.STRING_TYPE_INFO;
    }
  }
}
