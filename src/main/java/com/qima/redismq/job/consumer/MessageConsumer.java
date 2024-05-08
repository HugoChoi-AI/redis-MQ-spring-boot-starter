package com.qima.redismq.job.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qima.redismq.bean.RedisJob;
import com.qima.redismq.config.RedisMQProperties;
import com.qima.redismq.exception.FailedHandleMessageException;
import com.qima.redismq.exception.ServiceNotReachableException;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class MessageConsumer implements StreamListener<String, MapRecord<String, String, String>> {
    Logger log = org.slf4j.LoggerFactory.getLogger(MessageConsumer.class);

    protected ObjectMapper mapper = new ObjectMapper();
    protected RedisMQProperties redisMQProperties;
    @Getter
    protected String jobName;
    protected RedisJob redisJob;
    protected StringRedisTemplate redisTemplate;
    protected StreamOperations<String, String, String> opsForStream;


    protected MessageConsumer(RedisMQProperties redisMQProperties, StringRedisTemplate redisTemplate) {
        this.redisMQProperties = redisMQProperties;
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    void init() {
        redisJob = getRedisJob(jobName);
        opsForStream = redisTemplate.opsForStream();
        log.info("Initialized handler for consumer[{}]", redisJob.getConsumerName());
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        log.info("consumer[{}] is working on this job: {}", redisJob.getConsumerName(), jobName);
        try {
            claimMessages(redisJob.getStreamName(), redisJob.getConsumerGroup(), redisJob.getConsumerName(), 60, message.getId());
            handleMessage(message);
            acknowledge(message);
        } catch (ServiceNotReachableException e) {
            log.error("Service not reachable, will try again later by handlePendingMessage()");
        } catch (FailedHandleMessageException e) {
            log.error("Failed to handle message", e);
            increaseRetryTimes(message.getId().getValue());
        } catch (Exception e) {
            log.error("error in handling message", e);
        }
    }


    public RedisJob getRedisJob(String jobName) {
        Assert.hasText(jobName, "jobName must not be empty");
        return redisMQProperties.getJobs().values().stream().filter(job -> job.getName().equals(jobName)).findFirst().orElseThrow(() -> new RuntimeException(String.format("job %s not found", jobName)));
    }

    public void acknowledge(MapRecord<String, String, String> message) {
        log.info("Group[{}] consumer[{}] acknowledge message: {}", redisJob.getConsumerGroup(), redisJob.getConsumerName(), message.getId());
        clearRetryTimes(message.getId().getValue());
        redisTemplate.opsForStream().acknowledge(redisJob.getConsumerGroup(), message);
    }

    public void handlePendingMessage() {
        log.info("consumer[{}] is processing pending message of {}", redisJob.getConsumerName(), jobName);
        try {
            PendingMessages messages = opsForStream.pending(redisJob.getStreamName(), redisJob.getConsumerGroup(), Range.unbounded(), redisMQProperties.getMessagesPerPoll());
            log.info("got total {} pending messages.", messages.size());
            RecordId[] ids = messages.get().map(PendingMessage::getId).toArray(RecordId[]::new);
            List<MapRecord<String, String, String>> claimedMessages = claimMessages(redisJob.getStreamName(), redisJob.getConsumerGroup(), redisJob.getConsumerName(), 120L, ids);
            for (MapRecord<String, String, String> message : claimedMessages) {
                if (movedToDeadLetterStream(message)) {
                    continue;
                }
                onMessage(message);
            }

        } catch (Exception e) {
            log.error("error in processing pending message", e);
        }
    }

    public List<MapRecord<String, String, String>> claimMessages(String key, String group, String consumer, long seconds, RecordId... ids) {
        return redisTemplate.execute((RedisCallback<List<MapRecord<String, String, String>>>) connection -> {
            List<ByteRecord> byteRecords = connection.streamCommands().xClaim(Objects.requireNonNull(redisTemplate.getStringSerializer().serialize(key)), group, consumer, Duration.ofSeconds(seconds), ids);
            if (null==byteRecords){
                return Collections.emptyList();
            }
            return byteRecords.stream().map(byteRecord -> byteRecord.deserialize(RedisSerializer.string())).collect(Collectors.toList());
        });
    }

    public boolean movedToDeadLetterStream(MapRecord<String, String, String> message) {
        if (-1 != redisJob.getRetryTimesBeforeDead() && StringUtils.isNotBlank(redisJob.getDeadLetterStreamName())) {
            log.info("consumer[{}] is checking if need to move message to dead letter stream", redisJob.getConsumerName());
            int retryTimes = getRetryTimes(message.getId().getValue());
            log.info("retry times: {}", retryTimes);
            if (retryTimes >= redisJob.getRetryTimesBeforeDead()) {
                log.info("consumer[{}] is moving message to dead letter stream", redisJob.getConsumerName());
                StringRecord newRecord = StreamRecords.string(message.getValue()).withStreamKey(redisJob.getDeadLetterStreamName());
                redisTemplate.opsForStream().add(newRecord);
                redisTemplate.opsForStream().acknowledge(redisJob.getConsumerGroup(), message);
                return true;
            }
        }
        return false;
    }

    public int getRetryTimes(String messageId) {
        Object nullable = redisTemplate.opsForHash().get(redisJob.getRetryTimesKey(), redisJob.getConsumerGroup() + ":" + messageId);
        return nullable == null ? 0 : Integer.parseInt((String) nullable);
    }

    public void increaseRetryTimes(String messageId) {
        redisTemplate.opsForHash().increment(redisJob.getRetryTimesKey(), redisJob.getConsumerGroup() + ":" + messageId, 1);
    }

    public void clearRetryTimes(String messageId) {
        redisTemplate.opsForHash().delete(redisJob.getRetryTimesKey(), redisJob.getConsumerGroup() + ":" + messageId);
    }

    public abstract void handleMessage(MapRecord<String, String, String> message) throws ServiceNotReachableException, FailedHandleMessageException;


}
