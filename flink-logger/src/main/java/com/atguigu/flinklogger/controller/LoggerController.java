package com.atguigu.flinklogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Logan
 * @create 2021-07-26 13:33
 */
@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    @RequestMapping("/applog")
    public String getLogger(@RequestParam("param") String logStr){
        log.info(logStr);
        kafkaTemplate.send("ods_base_log",logStr);
        return "sucess";
    }
}
