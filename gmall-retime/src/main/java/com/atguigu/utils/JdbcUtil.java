package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Logan
 * @create 2021-08-01 16:01
 */
public class JdbcUtil {
  // 为了封装成通用的，查询的结果可能是多条，使用集合，集合中的数据类型可能有很多种，那么使用泛型，根据传进来的参数决定，通过SQL的方式查询更方便，因为我们现在正是测试，不需要转换大驼峰小驼峰，如果后面真正的开始做了，那么就需要转，这样才不会出现字段不符而报错的问题，可以闯入参数进行判断是否需要转换。
  public static <T> List<T> queryList(
      Connection connection, String querySql, Class<T> clz, boolean toCamel) {
    // 创建结果集
    List<T> result = new ArrayList<>();
    // 编译sql
    PreparedStatement preparedStatement = null;
    try {
      preparedStatement = connection.prepareStatement(querySql);
      // 执行查询
      ResultSet resultSet = preparedStatement.executeQuery();
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      // 遍历查询结果集，封装对象放入集合中
      while (resultSet.next()) {
        // 创建泛型对象
        T t = clz.newInstance();
        for (int i = 1; i < columnCount + 1; i++) {
          // 取出列名
          String columnName = metaData.getColumnName(i);
          // 因为我们hbase里面的字段为大写下划线，所以需要使用的话需要转换成小驼峰的格式
          if (toCamel) {
            columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
          }
          // 取出值
          String value = resultSet.getString(i);
          // 给对象赋值,通过官方提供的工具，通过反射的方式给Javabean填充属性
          BeanUtils.setProperty(t, columnName, value);
        }
        // 将对象添加到集合
        result.add(t);
      }
    } catch (Exception e) {
        e.printStackTrace();
    }finally{
        if (preparedStatement!=null){
            try{
                preparedStatement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
    // 返回结果集
    return result;
  }

  public static void main(String[] args) throws Exception {
      Class.forName(GmallConfig.PHOENIX_DRIVER);
      Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    System.out.println(
        queryList(
            connection,
            //TODO 根据下面的语句，我们可以再做一个优化，将表名提取出来，我们只需要传入id的参数即可，所以封装一个DIMUtil工具类
            "select * from GMALL210225_REALTIME.DIM_BASE_TRADEMARK",
            JSONObject.class,
            false));
    connection.close();
  }
}
