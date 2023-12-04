package com.qima.redismq.job.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qima.redismq.bean.RedisJob;
import com.qima.redismq.config.RedisMQProperties;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.Getter;
import org.slf4j.Logger;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
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

    protected ObjectMapper mapper =  new ObjectMapper();
    protected RedisMQProperties redisMQProperties;
    @Getter
    protected String jobName;
    protected RedisJob redisJob;
    protected StringRedisTemplate redisTemplate;
    protected StreamOperations<String, String, String> opsForStream;


    public MessageConsumer(RedisMQProperties redisMQProperties,
                    StringRedisTemplate redisTemplate){
        this.redisMQProperties = redisMQProperties;
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    void init(){
        redisJob = getRedisJob(jobName);
        opsForStream = redisTemplate.opsForStream();
        log.info("Initialized handler for consumer[{}]", redisJob.getConsumerName());
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        log.info("consumer[{}] is working on this job: {}", redisJob.getConsumerName(), jobName);
        handleMessage(message);
    }


    public RedisJob getRedisJob(String jobName){
        Assert.hasText(jobName, "jobName must not be empty");
        return redisMQProperties.getJobs().values().stream()
                .filter(job -> job.getName().equals(jobName))
                .findFirst().orElseThrow(()->new RuntimeException(String.format("job %s not found", jobName)));
    }

    public void acknowledge(MapRecord<String, String, String> message){
        log.info("Group[{}] consumer[{}] acknowledge message: {}", redisJob.getConsumerGroup(), redisJob.getConsumerName(), message.getId());
        redisTemplate.opsForStream().acknowledge(redisJob.getConsumerGroup(), message);
    }

    public void handlePendingMessage() {
        log.info("consumer[{}] is processing pending message of {}", redisJob.getConsumerName(), jobName);
        try {
            PendingMessages messages = opsForStream.pending(redisJob.getStreamName(), redisJob.getConsumerGroup(),
                                                            Range.unbounded(), redisMQProperties.getMessagesPerPoll());
            log.info("pending message size: {}", messages.size());
            for (PendingMessage pendingMessage : messages) {
                RedisAsyncCommands commands = (RedisAsyncCommands) Objects.requireNonNull(redisTemplate.getConnectionFactory())
                        .getConnection().getNativeConnection();
                CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                        .add(redisJob.getStreamName())
                        .add(redisJob.getConsumerGroup())
                        .add(redisJob.getConsumerName())
                        .add("10")
                        .add(pendingMessage.getIdAsString());
                commands.dispatch(CommandType.XCLAIM, new StatusOutput<>(StringCodec.UTF8), args);
                log.info("Message " + pendingMessage.getIdAsString() + " claimed by " + redisJob.getConsumerGroup() + ":" + redisJob.getConsumerName());

                List<MapRecord<String, String, String>> messagesToProcess = opsForStream
                        .range(redisJob.getStreamName(), Range.closed(pendingMessage.getIdAsString(), pendingMessage.getIdAsString()));
                if (CollectionUtils.isEmpty(messagesToProcess)) {
                    log.info("Can not find message with id: {}", pendingMessage.getIdAsString());
                    continue;
                }
                handleMessage(messagesToProcess.get(0));
            }
        } catch (Exception e) {
            log.error("error in processing pending message", e);
        }

    }

    public abstract void handleMessage(MapRecord<String, String, String> message);


}
