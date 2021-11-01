package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Logan
 * @create 2021-07-28 14:28
 */
public class BaseLogApp {
  public static void main(String[] args) throws Exception {
    // 创建执行环境
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
  /*  //开启ck
    env.enableCheckpointing(5000L);
    env.getCheckpointConfig().setAlignmentTimeout(10000L);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
    env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));*/
    //读取kafka的数据
    String topic ="ods_base_log";
    String groupId="BaseLogApp0225";
    DataStreamSource<String> kafkaDStream = env.addSource(MyKafkaUtil.getFlinkKafkaCounsumer(topic, groupId));

    //将数据转换成jsonObject
    OutputTag<String> dirtyOutputTag = new OutputTag<String>("DirtyData") {
    };
    SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDStream.process(new ProcessFunction<String, JSONObject>() {
      @Override
      public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //防止脏数据导致整个程序奔溃
        try {
          JSONObject jsonObject = JSONObject.parseObject(value);
          out.collect(jsonObject);
        } catch (Exception e) {
          ctx.output(dirtyOutputTag, value);
        }
      }
    });
    //TODO 将脏数据打印输出
    jsonObjDs.getSideOutput(dirtyOutputTag).print();

    //因为需要对每一个用户进行分析，所以按照mid进行分组
    KeyedStream<JSONObject, String> keyedStream = jsonObjDs.keyBy((KeySelector<JSONObject, String>) value -> value.getJSONObject("common").getString("mid"));

    //TODO 新老用户的校验
    SingleOutputStreamOperator<JSONObject> outputStreamOperator = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
      private ValueState<String> flagState;

      @Override
      public void open(Configuration parameters) throws Exception {
        //初始化状态
        flagState = getRuntimeContext().getState(new ValueStateDescriptor<String>("flag-state", String.class));
      }

      @Override
      public JSONObject map(JSONObject value) throws Exception {
        //获取isnew的标记
        String isNew = value.getJSONObject("common").getString("is_new");
        //判断标记是否为'1'
        if ("1".equals(isNew)) {
          //获取当前状态
          String state = flagState.value();
          if (state != null) {
            //将数据中的“1”改为“0”
            value.getJSONObject("common").put("is_new", "0");
          } else {
            //否则数据写入状态
            flagState.update("1");
          }
        }
        return value;
      }
    });
    //TODO 关于页面的输出
    //使用侧输出流对数据进行分流处理 页面-主流  启动-侧输出流 曝光-侧输出流
    OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
    };
    //{}使用这个的时候可以防止泛型檫除，因为out后面需要一个泛型，如果不加大括号，那么编译的时候识别不了这是jsonObject，所以需要加{}
    OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
    };
    SingleOutputStreamOperator<JSONObject> pageDs = outputStreamOperator.process(new ProcessFunction<JSONObject, JSONObject>() {
      @Override
      public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        //获取启动相关数据
        String start = value.getString("start");
        //判断是否为启动数据
        if (start != null && start.length() > 0) {
          //将数据写入start侧输出流
          ctx.output(startTag, value);
        } else {
          //将数据写入page主流
          out.collect(value);
          //获取曝光数据
          JSONArray displays = value.getJSONArray("displays");
          //可能页面中没有曝光，所以需要判断一下是否存在曝光数据。
          if (displays != null && displays.size() > 0) {
            //因为下面需要添加其他信息，所以在获取一下页面id
            String pageId = value.getJSONObject("common").getString("page_id");
            //遍历曝光数据并写出到display侧输出流中
            for (int i = 0; i < displays.size(); i++) {
              JSONObject display = displays.getJSONObject(i);
              //可以在曝光数据里面加一些其他信息
              display.put("page_id", pageId);
              //然后从侧输出流输出
              ctx.output(displayTag, display);
            }
          }

        }
      }
    });

    //TODO 将所有的数据汇总写入kafka中对应的主题
    DataStream<JSONObject> startDS = pageDs.getSideOutput(startTag);
    DataStream<JSONObject> displayDS = pageDs.getSideOutput(displayTag);
    startDS.print("Start>>>>>>>>");
    displayDS.print("Display>>>>>>>>");
    pageDs.print("Page>>>>>>>>>>>>>");

    String startTopic="dwd_start_log";
    String displayTopic="dwd_display_log";
    String pageTopic="dwd_page_log";
    //写入kafka
    startDS.map((MapFunction<JSONObject, String>) JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkafkaProducer(startTopic));
    displayDS.map((MapFunction<JSONObject, String>) JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkafkaProducer(displayTopic));
    pageDs.map((MapFunction<JSONObject, String>) JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkafkaProducer(pageTopic));

    //TODO 启动任务
    env.execute();
  }
}
