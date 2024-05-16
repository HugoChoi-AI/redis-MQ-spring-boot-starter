package com.qima.redismq.job;

import com.qima.redismq.bean.RedisJob;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Objects;

@Slf4j
public class InitUtil {

    private InitUtil() {
    }
    public static void createConsumerGroup(StringRedisTemplate redisTemplate,RedisJob job) {
        try {
            if (Boolean.FALSE.equals(redisTemplate.hasKey(job.getStreamName()))) {
                log.info("Stream:{} does not exist, create it first!", job.getStreamName());
                var commands = (RedisAsyncCommands) Objects.requireNonNull(redisTemplate.getConnectionFactory())
                        .getConnection()
                        .getNativeConnection();
                CommandArgs<String, String> commandArgs = new CommandArgs<>(StringCodec.UTF8)
                        .add(CommandKeyword.CREATE)
                        .add(job.getStreamName())
                        .add(job.getConsumerGroup())
                        .add("0")
                        .add("MKSTREAM");
                commands.dispatch(CommandType.XGROUP, new StatusOutput<>(StringCodec.UTF8), commandArgs);
            } else {
                log.info("Stream:{} already exists! Try to create Group:{}", job.getStreamName(), job.getConsumerGroup());
                redisTemplate.opsForStream().createGroup(job.getStreamName(), ReadOffset.from("0"), job.getConsumerGroup());
            }
        } catch (Exception e) {
            log.info("Group:{} already exists!", job.getConsumerGroup());
        }
    }
}
