package org.nereus.queue.constant;

/**
 * @description: Some constants
 * @author: nereus east
 * @data: 2020/3/22 15:03
 */
public class RedisBlockingDelayQueueConstant {

    /**
     * A complete queue contains keys similar to
     *
     * DEFAULT_BLOCKING_DELAY:QUEUE_WAITING
     * DEFAULT_BLOCKING_DELAY:QUEUE_CONTENT
     * DEFAULT_BLOCKING_DELAY:QUEUE_READY
     * DEFAULT_BLOCKING_DELAY:QUEUE_BACK
     *
     */
    public static final String DEFAULT_QUEUE_NAME = "DEFAULT_BLOCKING_DELAY";

    public static final String KEY_PRE_WAITING_SORTED_SET = ":QUEUE_WAITING";
    public static final String KEY_PRE_CONTENT_HASH = ":QUEUE_CONTENT";
    public static final String KEY_PRE_READY_LIST = ":QUEUE_READY";
    public static final String KEY_PRE_BACK_LIST = ":QUEUE_BACK";
}
