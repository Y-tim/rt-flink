package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * TODO 通过传入连接，表名，id; 先查询redis中的数据，如果存在就直接返回，不存在则查询hbase，然后写入缓存
 * @author Logan
 * @create 2021-08-01 18:56
 */
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection,String tableName,String id){
        //查询redis中的数据
        String rediskey="DIM:" +tableName+":"+id;
        Jedis jedis = Redisutil.getJedis();
        String dimInfo = jedis.get(rediskey);
        if(dimInfo!=null){
            JSONObject dimInfoJson = JSONObject.parseObject(dimInfo);
        //重置过期时间
        jedis.expire(rediskey,24*60*60);
        jedis.close();
        return dimInfoJson;
        }

        //完成sql的拼接
        String querySql="select * from "+ GmallConfig.HBASE_SCHEMA+ "."+tableName+" where id='"+id+"'";
    System.out.println(querySql);
    //编译查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        //将查询到的数据写入缓存
        JSONObject dimInfoJson = queryList.get(0);
        jedis.set(rediskey,dimInfoJson.toJSONString());
        //设置过期时间
        jedis.expire(rediskey,24*60*60);
        jedis.close();

        //返回结果数据
        return dimInfoJson;
    }

  public static void main(String[] args) throws Exception {
    //
      Class.forName(GmallConfig.PHOENIX_DRIVER);
      Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "28"));
    connection.close();
  }
}
