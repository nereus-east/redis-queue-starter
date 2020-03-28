package org.nereus.queue.core;

/**
 * @description: A serialized job class is not considered for the moment
 * @author: nereus east
 * @data: 2020/3/22 14:57
 */
public class StringDelayedJob extends AbstractDelayedJob<String> {

    public StringDelayedJob() {
    }

    public static StringDelayedJob build(String message, long expectedTime){
        return new StringDelayedJob(message, expectedTime);
    }

    public StringDelayedJob(String message, long expectedTime) {
        super(message, expectedTime);
    }
}
