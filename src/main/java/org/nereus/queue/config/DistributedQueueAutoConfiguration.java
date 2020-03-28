package org.nereus.queue.config;

import org.nereus.queue.core.RedisBlockingDelayQueue;
import org.nereus.queue.core.StringDelayedJob;
import org.nereus.queue.constant.RedisBlockingDelayQueueConstant;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * @author nereus east
 * @description:
 * @date 2020/3/19 16:04
 **/
@Configuration
public class DistributedQueueAutoConfiguration {

    @Bean
    @ConditionalOnClass(RedisBlockingDelayQueue.class)
    @ConditionalOnMissingBean(name = "redisBlockingDelayQueue")
    public RedisBlockingDelayQueue redisBlockingDelayQueue(@Qualifier("queueRedisTemplate") StringRedisTemplate queueRedisTemplate) {
        return new RedisBlockingDelayQueue<>(queueRedisTemplate,
                RedisBlockingDelayQueueConstant.DEFAULT_QUEUE_NAME,
                StringDelayedJob.class);
    }
}
