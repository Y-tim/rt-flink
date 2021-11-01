import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;

/**
 * @author Logan
 * @create 2021-08-03 0:38
 */
public class UserJumpDetailApp {
  public static void main(String[] args) throws Exception {
    // 1、获取执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    // 2、开启ck
    env.enableCheckpointing(5000L);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
    env.getCheckpointConfig().setAlignmentTimeout(10000L);
    env.setStateBackend(new FsStateBackend(""));
    // 3、kafka中获取数据dwd_page_log
    String sourTopic="dwd_page_log";
    String groupId="UserJumpDetailApp";
    String sinkTopic="dwm_user_jump_detail";
    DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaCounsumer(sourTopic, groupId));
    // 4、封装成jsonObject并分组指定watermark
    KeyedStream<JSONObject, String> keyedStream = kafkaDs.map(JSONObject::parseObject).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
      @Override
      public long extractTimestamp(JSONObject element, long recordTimestamp) {
        return element.getLong("ts");
      }
    })).keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
    // 5、指定模式
    Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
      @Override
      public boolean filter(JSONObject value) throws Exception {

        return value.getJSONObject("common").getString("last_page_id") == null;
      }
    }).times(2)
            .consecutive()
            .within(Time.seconds(10));
    // 6、将模式作用于流上
    PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
    // 7、提取匹配上的数据
    OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("timeOut");
    SingleOutputStreamOperator<JSONObject> streamOperator = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
      @Override
      public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
        return map.get("start").get(0);
      }
    }, new PatternSelectFunction<JSONObject, JSONObject>() {
      @Override
      public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
        return map.get("start").get(0);
      }
    });
    // 8、将侧输出流的数据和主流的数据Union
    DataStream<JSONObject> unionDS = streamOperator.union(streamOperator.getSideOutput(outputTag));
    // 8、写入kafka中的dwm_user_jump_detail
    unionDS.map(JSONAware::toJSONString)
            .addSink(MyKafkaUtil.getFlinkafkaProducer(sinkTopic));
    // 9、启动任务
    env.execute();

  }
}
