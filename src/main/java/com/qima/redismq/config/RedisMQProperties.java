package com.qima.redismq.config;

import com.qima.redismq.bean.RedisJob;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Data
@ConfigurationProperties(prefix = "redis.mq")
public class RedisMQProperties {

    private int messagesPerPoll;
    private int pollTimeout;
    private Map<String, RedisJob> jobs;
}
