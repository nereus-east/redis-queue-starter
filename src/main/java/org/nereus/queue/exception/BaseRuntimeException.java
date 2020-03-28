package org.nereus.queue.exception;

/**
 * @Description:
 * @author: nereus east
 * @Data: 2019/7/24 11:23
 */
public abstract class BaseRuntimeException extends RuntimeException {
    private String code;

    public BaseRuntimeException(String message, String code) {
        super(message);
        this.code = code;
    }

    public BaseRuntimeException(String message, String code, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
