package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author Logan
 * @create 2021-08-01 20:34
 */
public interface DimJoinFunction<T> {
    String getKey(T input);
    void join(T input, JSONObject dimInfo) throws ParseException;

}
