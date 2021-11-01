import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @author Logan
 * @create 2021-08-03 1:28
 */
public class OrderWideApp {
  public static void main(String[] args) throws Exception {
    // 1、获取执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    //2、开启ck
    env.enableCheckpointing(5000L);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
    env.getCheckpointConfig().setAlignmentTimeout(10000L);
    env.setStateBackend(new FsStateBackend(""));
    //3、获取kafka中的dwd_order_info\dwd_order_detail
    String orderInfoSourceTopic = "dwd_order_info";
    String orderDetailSourceTopic = "dwd_order_detail";
    String orderWideSinkTopic = "dwm_order_wide";
    String groupId = "order_wide_group0225";
    DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaCounsumer(orderInfoSourceTopic, groupId));
    DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaCounsumer(orderDetailSourceTopic, groupId));
    //4、各自封装成JavaBean，并指定watermark
    SingleOutputStreamOperator<OrderInfo> orderInfoDDS = orderInfoStrDS.map(line -> {
      OrderInfo orderInfo = JSONObject.parseObject(line, OrderInfo.class);
      String create_time = orderInfo.getCreate_time();
      String[] dates = create_time.split(" ");
      orderInfo.setCreate_date(dates[0]);
      orderInfo.setCreate_hour(dates[1].split(":")[0]);
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:s");
      orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
      return orderInfo;

    }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
      @Override
      public long extractTimestamp(OrderInfo element, long recordTimestamp) {
        return element.getCreate_ts();
      }
    }));
    SingleOutputStreamOperator<OrderDetail> orderDetailDDS = orderDetailStrDS.map(line -> {
      OrderDetail orderDetail = JSONObject.parseObject(line, OrderDetail.class);
      String create_time = orderDetail.getCreate_time();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
      return orderDetail;
    }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
      @Override
      public long extractTimestamp(OrderDetail element, long recordTimestamp) {
        return element.getCreate_ts();
      }
    }));
    //5、双流join，使用intervaljoin
    SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDDS.keyBy(OrderInfo::getId)
            .intervalJoin(orderDetailDDS.keyBy(OrderDetail::getOrder_id))
            .between(Time.seconds(-5), Time.seconds(5))
            .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
              @Override
              public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                out.collect(new OrderWide(orderInfo, orderDetail));
              }
            });
    //6、关联维度
    SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
      @Override
      public String getKey(OrderWide orderWide) {
        return orderWide.getUser_id().toString();
      }

      @Override
      public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
        //提取字段
        String genoer = dimInfo.getString("GENOER");
        orderWide.setUser_gender(genoer);
        String birthday = dimInfo.getString("BIRTHDAY");
        long currTs = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long ts = sdf.parse(birthday).getTime();
        Long age = (currTs - ts) / (1000L * 60 * 60 * 24 * 365);
        orderWide.setUser_age(age.intValue());
      }
    }, 60, TimeUnit.SECONDS);
    SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
      @Override
      public String getKey(OrderWide orderWide) {
        return orderWide.getProvince_id().toString();
      }

      @Override
      public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
        String name = dimInfo.getString("NAME");
        String area_code = dimInfo.getString("AREA_CODE");
        String iso_code = dimInfo.getString("ISO_CODE");
        String iso_3166_2 = dimInfo.getString("ISO_3166_2");

        orderWide.setProvince_name(name);
        orderWide.setProvince_area_code(area_code);
        orderWide.setProvince_iso_code(iso_code);
        orderWide.setProvince_3166_2_code(iso_3166_2);
      }
    }, 60, TimeUnit.SECONDS);

    SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
      @Override
      public String getKey(OrderWide orderWide) {
        return orderWide.getSku_id().toString();
      }

      @Override
      public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
        orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
        orderWide.setTm_id(dimInfo.getLong("TM_ID"));
      }
    }, 60, TimeUnit.SECONDS);
    //5.4 关联SPU维度
    SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
            orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
              @Override
              public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
              }

              @Override
              public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getSpu_id());
              }
            }, 60, TimeUnit.SECONDS);

    //5.5 关联品牌维度
    SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
            orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
              @Override
              public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                orderWide.setTm_name(jsonObject.getString("TM_NAME"));
              }

              @Override
              public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getTm_id());
              }
            }, 60, TimeUnit.SECONDS);

    //5.6 关联品类维度
    SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
            orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
              @Override
              public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                orderWide.setCategory3_name(jsonObject.getString("NAME"));
              }

              @Override
              public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getCategory3_id());
              }
            }, 60, TimeUnit.SECONDS);
    //7、写入kafka中的主题orderwide
    orderWideWithCategory3DS.map(JSONObject::toJSONString)
            .addSink(MyKafkaUtil.getFlinkafkaProducer(orderWideSinkTopic));
    //8、启动任务
    env.execute();
  }
}
