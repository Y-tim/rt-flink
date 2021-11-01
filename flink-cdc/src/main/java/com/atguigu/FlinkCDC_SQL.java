package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Logan
 * @create 2021-07-27 20:11
 */
public class FlinkCDC_SQL {
  public static void main(String[] args) throws Exception {
    //创建环境
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
      //获取表的执行环境
      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
      //执行sql
     tableEnv.executeSql("CREATE TABLE user_info(\n" +
              " id INT ,\n" +
              " tm_name STRING,\n" +
              " logo_url STRING \n" +
              ") WITH (\n" +
              " 'connector' = 'mysql-cdc',\n" +
              " 'hostname' = 'hadoop102',\n" +
              " 'port' = '3306',\n" +
              " 'username' = 'root',\n" +
              " 'password' = '123456',\n" +
              " 'database-name' = 'gmall-flink0225',\n" +
              " 'table-name' = 'base_trademark'\n" +
              ")");
      //查询并打印结果
      TableResult tableResult = tableEnv.executeSql("select * from user_info");
      tableResult.print();
      //执行任务
      env.execute();
  }
}
