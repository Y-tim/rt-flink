package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;

/**
 * @author Logan
 * @create 2021-07-29 14:44
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

  // 定义状态描述
  private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
  // 定义phinx的连接
  private Connection connection;

  // 定义一个测输出流属性
  private OutputTag<JSONObject> hbaseTage;

  public TableProcessFunction() {}

  @Override
  public void open(Configuration parameters) throws Exception {
    Class.forName(GmallConfig.PHOENIX_DRIVER);
    connection=DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
  }

  public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hbaseTage) {
    this.mapStateDescriptor = mapStateDescriptor;
    this.hbaseTage = hbaseTage;
  }

  // value:{"db":"","tn":"","data":{"sourceTable":"",...},"before":{},"type":"insert"}
  @Override
  public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out)
      throws Exception {
    // 获取并解析数据
    JSONObject jsonObject = JSONObject.parseObject(value);
    TableProcess tableProcess =
        JSONObject.parseObject(jsonObject.getString("data"), TableProcess.class);

    // 校验表是否存在，如果不存在则建表
    String sinkType = tableProcess.getSinkType();
    String type = jsonObject.getString("type");
    if ("insert".equals(type) && TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
      checkTable(
          tableProcess.getSinkTable(),
          tableProcess.getSinkColumns(),
          tableProcess.getSinkPk(),
          tableProcess.getSinkExtend());
    }
    BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
    String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
    broadcastState.put(key, tableProcess);
  }
  // 建表语句：create table if not exists db.t(id varchar primary key,tm_name varchar)...
  private void checkTable(String sinkTable, String SinkColumns, String sinkPK, String sinkExtend) {
    // 有的表没有，所以需要给一个默认值
    if (sinkPK == null) {
      sinkPK = "id";
    }
    if (sinkExtend == null) {
      sinkExtend = "";
    }
    // 建表可能会出现错误所以需要进行捕获异常
    try {
      StringBuilder createTableSql =
          new StringBuilder("create table if not exists ")
              // 库,hbase中叫namespace，phinx中叫schema
              .append(GmallConfig.HBASE_SCHEMA)
              .append(".")
              .append(sinkTable)
              .append("(");
      String[] colums = SinkColumns.split(",");
      for (int i = 0; i < colums.length; i++) {
        String colum = colums[i];
        // 判断是否为主键
        if (sinkPK.equals(colum)) {
          createTableSql.append(colum).append(" varchar primary key");
        } else {
          createTableSql.append(colum).append(" varchar");
        }

        // 判断是否为最后一个字段
        if (i < colums.length - 1) {
          createTableSql.append(",");
        }else{
          createTableSql.append(")").append(sinkExtend);
        }
      }
      //打印建表语句
      System.out.println(createTableSql);
      // 预编译sql并赋值
      PreparedStatement prepareStatement = connection.prepareStatement(createTableSql.toString());
      // 执行sql
      prepareStatement.execute();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("建表" + sinkTable + "失败!");
    }
  }

  @Override
  public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out)
      throws Exception {
    // 获取广播的配置数据
    ReadOnlyBroadcastState<String, TableProcess> broadcastState =
        ctx.getBroadcastState(mapStateDescriptor);
    // 通过正常流的连接字段查询状态表中的数据
    String key = value.getString("tableName") + "_" + value.getString("type");
    TableProcess tableProcess = broadcastState.get(key);
    // 首先获取的到tableprocess不能为空
    if (tableProcess != null) {
      // 过滤字段
      filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());
      // 分流
      String sinkType = tableProcess.getSinkType();
      // TODO 因为后面需要判断需要写入什么地方
      value.put("sinkTable", tableProcess.getSinkTable());
      if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
        // 将数据写入侧输出流
        ctx.output(hbaseTage,value);
      } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
        // 将数据写入主流
        out.collect(value);
      } else {
        System.out.println(key + "不存在！");
      }
    }
  }

  /** 需要传入的参数为：正常数据流中的data中的字段，以及状态表中的列信息 */
  private void filterColumn(JSONObject data, String sinkColumns) {
    // 状态表中的columns需要将指定的列切开
    String[] colums = sinkColumns.split(",");
    // 因为字符串数组不存在无法判断是否包含要查询的值
    List<String> columnList = Arrays.asList(colums);
    // 获取data的单个值
    Set<Map.Entry<String, Object>> entries = data.entrySet();
    // 遍历
    Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Object> next = iterator.next();
      if (!columnList.contains(next.getKey())) {
        iterator.remove();
      }
    }
  }
}
