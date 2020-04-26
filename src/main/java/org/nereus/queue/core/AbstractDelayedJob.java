package org.nereus.queue.core;

import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @description: Queue element metadata
 * @author: nereus east
 * @data: 2020/3/19 16:10
 */
public abstract class AbstractDelayedJob implements Delayed {

    /**
     * The unique id of the job
     */
    private String id;

    private long creationTime = System.currentTimeMillis();

    /**
     * The remaining number of times that can be awakened
     * Initial value takes effect at {@code RedisBlockingDelayQueue : put of offer}, here is for query only
     */
    private int lives;

    /**
     * Number of times that have been awakened
     */
    private int lostLives = 0;


    /**
     * Execution timeout in milliseconds
     * Initial value takes effect at {@code RedisBlockingDelayQueue : put of offer}, here is for query only
     */
    private long timeout;

    /**
     * Indicate which topic the job belongs to, for caching only
     */
    private String topic = "";

    /**
     * The time the task is enabled to execute in milliseconds
     */
    private long expectedTime;

    /**
     * The actual time the job was received by the server
     */
    private long receiveTime;

    /**
     * For the first time interval
     */
    private long intervals;

    public AbstractDelayedJob() {
    }

    public AbstractDelayedJob(long expectedTime) {
        this(UUID.randomUUID().toString(), expectedTime);
    }

    public AbstractDelayedJob(String id, long expectedTime) {
        this.intervals = expectedTime - creationTime;
        this.id = id;
        this.expectedTime = expectedTime;
    }

    public String getId() {
        return id;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public int getLives() {
        return lives;
    }

    public void setLives(int lives) {
        this.lives = lives;
    }

    public int getLostLives() {
        return lostLives;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getExpectedTime() {
        return expectedTime;
    }

    public long getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(long receiveTime) {
        this.receiveTime = receiveTime;
    }

    public long getIntervals() {
        return intervals;
    }

    /**
     * When the task fails, this method will get the retry interval. You can override this method for custom configuration.
     */
    public long computeRetryInterval() {
        return lostLives * lostLives * intervals;
    }

    public final int loseOneLifeAndGet() {
        this.lostLives++;
        expectedTime = System.currentTimeMillis() + computeRetryInterval();
        return --this.lives;
    }


    @Override
    public final long getDelay(TimeUnit unit) {
        return unit.convert(expectedTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public final int compareTo(Delayed other) {
        if (other == null) {
            return -1;
        }
        if (this == other) {
            return 0;
        }
        long d = (getDelay(TimeUnit.MILLISECONDS) -
                other.getDelay(TimeUnit.MILLISECONDS));
        return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
    }
}
