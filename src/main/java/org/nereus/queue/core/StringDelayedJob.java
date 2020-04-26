package org.nereus.queue.core;

/**
 * @description: A serialized job class is not considered for the moment
 * @author: nereus east
 * @data: 2020/3/22 14:57
 */
public class StringDelayedJob extends AbstractDelayedJob {

    private String message;

    public static StringDelayedJob build(String message, long expectedTime) {
        return new StringDelayedJob(message, expectedTime);
    }

    public static StringDelayedJob build(String message, String id, long expectedTime) {
        return new StringDelayedJob(message, id, expectedTime);
    }

    public StringDelayedJob() {
    }

    public StringDelayedJob(String message, long expectedTime) {
        super(expectedTime);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public StringDelayedJob(String message, String id, long expectedTime) {
        super(id, expectedTime);
        this.message = message;
    }
}
