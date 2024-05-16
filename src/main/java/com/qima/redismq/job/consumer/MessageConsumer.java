package com.qima.redismq.job.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qima.redismq.bean.RedisJob;
import com.qima.redismq.config.RedisMQProperties;
import com.qima.redismq.exception.FailedHandleMessageException;
import com.qima.redismq.exception.ServiceNotReachableException;
import com.qima.redismq.job.InitUtil;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.ByteRecord;
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

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
        return redisMQProperties.getJobs().values().stream()
                .filter(job -> job.getName().equals(jobName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException(String.format("job %s not found", jobName)));
    }

    public void acknowledge(MapRecord<String, String, String> message) {
        log.info("Group[{}] consumer[{}] acknowledge message: {}", redisJob.getConsumerGroup(), redisJob.getConsumerName(), message.getId());
        clearRetryTimes(message.getId().getValue());
        redisTemplate.opsForStream().acknowledge(redisJob.getConsumerGroup(), message);
    }

    public void handlePendingMessage() {
        log.info("consumer[{}] is processing pending message of {}", redisJob.getConsumerName(), jobName);
        try {
            InitUtil.createConsumerGroup(redisTemplate, redisJob);
            PendingMessages pendingMessages = opsForStream.pending(redisJob.getStreamName(),
                                                                   redisJob.getConsumerGroup(),
                                                                   Range.unbounded(),
                                                                   redisMQProperties.getMessagesPerPoll());
            log.info("got total {} pending messages.", pendingMessages.size());
            if (!pendingMessages.isEmpty()) {
                for (PendingMessage pendingMessage : pendingMessages) {
                    if (pendingMessage.getElapsedTimeSinceLastDelivery().compareTo(Duration.ofSeconds(redisJob.getClaimSeconds()))<0){
                        log.info("Elapsed time for message[{}] is too short, skipping in this round.", pendingMessage.getId());
                        continue;
                    }
                    List<MapRecord<String, String, String>> claimedMessages = claimMessages(redisJob.getStreamName(),
                                                                                            redisJob.getConsumerGroup(),
                                                                                            redisJob.getConsumerName(),
                                                                                            redisJob.getClaimSeconds(), pendingMessage.getId());
                    if (!claimedMessages.isEmpty()) {
                        log.info("consumer[{}] claimed pending message: {}", redisJob.getConsumerName(), pendingMessage.getId());
                        MapRecord<String, String, String> message = claimedMessages.get(0);
                        if (!movedToDeadLetterStream(message)) {
                            onMessage(message);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("error in processing pending message", e);
        }
    }

    public List<MapRecord<String, String, String>> claimMessages(String key, String group, String consumer, long seconds, RecordId... ids) {
        return redisTemplate.execute((RedisCallback<List<MapRecord<String, String, String>>>) connection -> {
            List<ByteRecord> byteRecords = connection.streamCommands().xClaim(
                    Objects.requireNonNull(redisTemplate.getStringSerializer().serialize(key)), group, consumer,
                    Duration.ofSeconds(seconds), ids);
            if (null == byteRecords) {
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
