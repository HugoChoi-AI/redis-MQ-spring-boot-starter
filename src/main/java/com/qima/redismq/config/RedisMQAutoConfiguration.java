package com.qima.redismq.config;

import com.qima.redismq.job.InitRedisMQRegistrar;
import com.qima.redismq.job.consumer.MessageConsumer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

@Configuration
@EnableConfigurationProperties(value = RedisMQProperties.class)
public class RedisMQAutoConfiguration {

    @Bean
    public InitRedisMQRegistrar initRedisMQRegistrar(RedisMQProperties redisMQProperties,
                                                     List<MessageConsumer> consumers,
                                                     StringRedisTemplate redisTemplate,
                                                     Environment environment) {
        return new InitRedisMQRegistrar(redisMQProperties, consumers, redisTemplate, environment);
    }
}
