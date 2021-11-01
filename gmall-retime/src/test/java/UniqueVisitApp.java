import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * @author Logan
 * @create 2021-08-02 23:18
 */
public class UniqueVisitApp {
  public static void main(String[] args) throws Exception {
    // 1、获取执行环境
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    // 2、开启ck
    env.enableCheckpointing(5000L);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
    env.getCheckpointConfig().setAlignmentTimeout(10000L);
    env.setStateBackend(new FsStateBackend(""));
    // 3、从kafka中的dwd_page_log
    String pageTopic = "dwd_page_group";
    String groupId = "UniqueVisitApp0225";
    String sinkTopic = "dwm_unique_visit";
    DataStreamSource<String> pageDS =
        env.addSource(MyKafkaUtil.getFlinkKafkaCounsumer(pageTopic, groupId));
    // 4、封装成jsonObject
    SingleOutputStreamOperator<JSONObject> jsonDS = pageDS.map(JSONObject::parseObject);
    // 5、按照mid进行分组
    KeyedStream<JSONObject, String> keyedStream =
        jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
    // 6、去重
    SingleOutputStreamOperator<JSONObject> filterDS =
        keyedStream.filter(
            new RichFilterFunction<JSONObject>() {
              private ValueState<String> tsSate;
              private SimpleDateFormat sdf;

              @Override
              public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor =
                    new ValueStateDescriptor<>("ts-state", String.class);
                StateTtlConfig ttlConfig =
                    new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                tsSate=getRuntimeContext().getState(stateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
              }

              @Override
              public boolean filter(JSONObject value) throws Exception {
                // 获取上一个页面为的值
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                // 判断上一个页面是否为null，如果为null或者长度小于等于0，那么在判断
                if (lastPageId == null) {
                  // 获取状态中的时间
                  String ts = tsSate.value();
                  String currTs = sdf.format(value.getString("ts"));
                  // 判断，状态为null或者状态中的时间与当前时间不同，则更新并返回true
                  if (ts == null || currTs.equals(ts)) {
                    tsSate.update(currTs);
                    return true;
                  } else {
                    return false;
                  }
                } else {
                  return false;
                }
              }
            });
    // 7、写入kafka
      filterDS.map(JSONAware::toJSONString)
              .addSink(MyKafkaUtil.getFlinkafkaProducer(sinkTopic));
    // 启动任务
env.execute();
  }
}
