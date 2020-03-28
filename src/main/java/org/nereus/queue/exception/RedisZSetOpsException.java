package org.nereus.queue.exception;

/**
 * @Description:
 * @author: nereus east
 * @Data: 2020/3/23 16:35
 */
public class RedisZSetOpsException extends BaseRuntimeException {
    private static final String code = "REDIS_ZSET_OPERATE_EXCEPTION";

    public RedisZSetOpsException(String message) {
        super(message, code);
    }

    public RedisZSetOpsException(String message, Throwable cause) {
        super(message, code, cause);
    }
}
