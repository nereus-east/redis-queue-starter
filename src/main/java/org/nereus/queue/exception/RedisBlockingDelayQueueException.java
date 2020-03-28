package org.nereus.queue.exception;

/**
 * @Description:
 * @author: nereus east
 * @Data: 2020/3/23 16:46
 */
public class RedisBlockingDelayQueueException extends BaseRuntimeException {

    private static final String code = "REDIS_BLOCKING_DELAY_QUEUE_EXCEPTION";

    public RedisBlockingDelayQueueException(String message) {
        super(message, code);
    }

    public RedisBlockingDelayQueueException(String message, Throwable cause) {
        super(message, code, cause);
    }
}
