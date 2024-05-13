package com.qima.redismq.bean;

import lombok.Data;

@Data
public class RedisJob {
    private boolean enabled;
    private String name;
    private String cron;
    private String streamName;
    private String consumerGroup;
    private String consumerName;
    private String deadLetterStreamName;
    private int retryTimesBeforeDead;
    private long claimSeconds;
    private String retryTimesKey ="retryTimes";
}
