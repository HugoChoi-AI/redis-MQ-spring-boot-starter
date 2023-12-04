package com.qima.redismq.job;

import com.qima.redismq.bean.RedisJob;
import com.qima.redismq.config.RedisMQProperties;
import com.qima.redismq.job.consumer.MessageConsumer;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.CronTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class InitRedisMQRegistrar implements InitializingBean, SchedulingConfigurer, DisposableBean {
    private final Environment environment;
    private final RedisMQProperties redisMQProperties;
    private final List<MessageConsumer> consumers;
    private final StringRedisTemplate redisTemplate;

    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> messageListenerContainer;
    private List<CronTask> cronTasks;
    private String instanceIdentity = UUID.randomUUID().toString();

    public InitRedisMQRegistrar(RedisMQProperties redisMQProperties, List<MessageConsumer> consumers, StringRedisTemplate redisTemplate, Environment environment) {
        this.redisMQProperties = redisMQProperties;
        this.consumers = consumers;
        this.redisTemplate = redisTemplate;
        this.environment = environment;
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("MessageListenerRegistrar initializing...");
        try{
            instanceIdentity = StringUtils.join(getIp(), ":", environment.getProperty("server.port"));
        } catch (Exception e) {
            log.info("Failed to get host address to identify this instance, use a random one instead.", e);
        }
        this.messageListenerContainer = StreamMessageListenerContainer
                .create(Objects.requireNonNull(this.redisTemplate.getConnectionFactory()), StreamMessageListenerContainer
                        .StreamMessageListenerContainerOptions
                        .builder()
                        .batchSize(redisMQProperties.getMessagesPerPoll())
                        .pollTimeout(Duration.ofMillis(redisMQProperties.getPollTimeout()))
                        .serializer(new StringRedisSerializer())
                        .build());

        Collection<RedisJob> jobs = redisMQProperties.getJobs().values();
        cronTasks = new ArrayList<>(jobs.size());
        jobs.forEach(this::initJobContainer);
        this.messageListenerContainer.start();
    }

    private void initJobContainer(RedisJob job) {
        job.setConsumerName(job.getConsumerName() + "-" + instanceIdentity);
        createConsumerGroup(job);
        if (job.isEnabled()) {
            registerMessageConsumers(job);
        }
    }

    private String getIp() {
        String ip = environment.getProperty("spring.cloud.client.ip-address");
        if (StringUtils.isBlank(ip)) {
            try(final DatagramSocket socket = new DatagramSocket()) {
                socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
                ip =  socket.getLocalAddress().getHostAddress();
            } catch (Exception e) {
                log.info("Failed to ip", e);
            }
        }
        return ip;
    }

    private void createConsumerGroup(RedisJob job) {
        try {
            if (Boolean.FALSE.equals(redisTemplate.hasKey(job.getStreamName()))) {
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
                redisTemplate.opsForStream().createGroup(job.getStreamName(), ReadOffset.from("0"), job.getConsumerGroup());
            }
        } catch (Exception e) {
            log.info("Group:{} already exists!", job.getConsumerGroup());
        }
    }

    private void registerMessageConsumers(RedisJob job) {
        consumers.stream()
                .filter(consumer -> consumer.getJobName().equals(job.getName()))
                .findFirst()
                .ifPresent(consumer -> {
                    this.messageListenerContainer.receive(Consumer.from(job.getConsumerGroup(), job.getConsumerName()),
                                                          StreamOffset.create(job.getStreamName(), ReadOffset.lastConsumed()), consumer);
                    log.info("Consumer:[{}] is ready to consume messages from stream:[{}]", job.getConsumerName(), job.getStreamName());
                    cronTasks.add(new CronTask(consumer::handlePendingMessage, job.getCron()));
                    log.info("Add cron task for job:{} with cron:{}", job.getName(), job.getCron());
                });
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        log.info("Ready to schedule tasks...");
        cronTasks.forEach(taskRegistrar::addCronTask);
    }

    @Override
    public void destroy() throws Exception {
        log.info("destroying...");
        if (this.messageListenerContainer != null) {
            this.messageListenerContainer.stop();
        }
    }
}
