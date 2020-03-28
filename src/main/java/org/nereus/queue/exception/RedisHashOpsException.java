package org.nereus.queue.exception;

/**
 * @Description:
 * @author: nereus east
 * @Data: 2020/3/23 16:35
 */
public class RedisHashOpsException extends BaseRuntimeException {
    private static final String code = "REDIS_HASH_OPERATE_EXCEPTION";

    public RedisHashOpsException(String message) {
        super(message, code);
    }

    public RedisHashOpsException(String message, Throwable cause) {
        super(message, code, cause);
    }
}
