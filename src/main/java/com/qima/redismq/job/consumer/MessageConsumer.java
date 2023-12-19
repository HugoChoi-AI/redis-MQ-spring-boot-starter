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
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;

public abstract class MessageConsumer implements StreamListener<String, MapRecord<String, String, String>> {
    Logger log = org.slf4j.LoggerFactory.getLogger(MessageConsumer.class);

    protected ObjectMapper mapper = new ObjectMapper();
    protected RedisMQProperties redisMQProperties;
    @Getter
    protected String jobName;
    protected RedisJob redisJob;
    protected StringRedisTemplate redisTemplate;
    protected StreamOperations<String, String, String> opsForStream;


    public MessageConsumer(RedisMQProperties redisMQProperties, StringRedisTemplate redisTemplate) {
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
            increaseFailedTimes(message);
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
        redisTemplate.opsForStream().acknowledge(redisJob.getConsumerGroup(), message);
    }

    public void handlePendingMessage() {
        log.info("consumer[{}] is processing pending message of {}", redisJob.getConsumerName(), jobName);
        try {
            PendingMessages messages = opsForStream.pending(redisJob.getStreamName(), redisJob.getConsumerGroup(), Range.unbounded(), redisMQProperties.getMessagesPerPoll());
            log.info("pending message size: {}", messages.size());
            for (PendingMessage pendingMessage : messages) {
                MapRecord<String, String, String> message = getOneMessageById(pendingMessage.getIdAsString());
                if (message == null) {
                    log.info("Can not find message with id: {}", pendingMessage.getIdAsString());
                    continue;
                }
                if (movedToDeadLetterStream(message)) {
                    continue;
                }
                try {
                    handleMessage(message);
                    acknowledge(message);
                } catch (ServiceNotReachableException e) {
                    log.error("Service not reachable, will try again later");
                    break;
                } catch (FailedHandleMessageException e) {
                    log.error("Failed to handle message", e);
                    increaseFailedTimes(message);
                }
            }

        } catch (Exception e) {
            log.error("error in processing pending message", e);
        }
    }

    public MapRecord<String, String, String> getOneMessageById(String messageId) {
        RedisAsyncCommands commands = (RedisAsyncCommands) Objects.requireNonNull(redisTemplate.getConnectionFactory()).getConnection().getNativeConnection();
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).add(redisJob.getStreamName()).add(redisJob.getConsumerGroup()).add(redisJob.getConsumerName()).add("10").add(messageId);
        commands.dispatch(CommandType.XCLAIM, new StatusOutput<>(StringCodec.UTF8), args);
        log.info("Message " + messageId + " claimed by " + redisJob.getConsumerGroup() + ":" + redisJob.getConsumerName());
        List<MapRecord<String, String, String>> messagesToProcess = opsForStream.range(redisJob.getStreamName(), Range.closed(messageId, messageId));
        return CollectionUtils.isEmpty(messagesToProcess) ? null : messagesToProcess.get(0);

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
        return nullable == null ? 0 : (int) nullable;
    }

    public void increaseFailedTimes(MapRecord<String, String, String> message) {
        redisTemplate.opsForHash().increment(redisJob.getRetryTimesKey(), redisJob.getConsumerGroup() + ":" + message.getId(), 1);
    }

    public abstract void handleMessage(MapRecord<String, String, String> message) throws ServiceNotReachableException, FailedHandleMessageException;


}
