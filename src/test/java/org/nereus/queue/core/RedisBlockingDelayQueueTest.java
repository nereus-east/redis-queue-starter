package org.nereus.queue.core;

import org.nereus.queue.RedisConfig;
import org.nereus.queue.exception.RedisBlockingDelayQueueException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.function.Consumer;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@SpringBootApplication(scanBasePackageClasses = RedisConfig.class)
public class RedisBlockingDelayQueueTest {

    @Autowired
    RedisBlockingDelayQueue<StringDelayedJob> redisBlockingDelayQueue;

    @Test
    public void size() {
        redisBlockingDelayQueue.clear();
        redisBlockingDelayQueue.put(StringDelayedJob.build("testMessage1", System.currentTimeMillis() + 10000));
        redisBlockingDelayQueue.put(StringDelayedJob.build("testMessage2", System.currentTimeMillis() + 10000));
        redisBlockingDelayQueue.put(StringDelayedJob.build("testMessage3", System.currentTimeMillis() + 10000));
        redisBlockingDelayQueue.put(StringDelayedJob.build("testMessage4", System.currentTimeMillis() + 10000));
        assertEquals(4, redisBlockingDelayQueue.size());
        redisBlockingDelayQueue.clear();
    }

    @Test
    public void put() throws InterruptedException {
        redisBlockingDelayQueue.clear();

        long putTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", putTime + 1000);
        redisBlockingDelayQueue.put(putJob);
        StringDelayedJob takeJob = redisBlockingDelayQueue.take();
        assertTrue(System.currentTimeMillis() - putTime > 1000);
        assertEquals(putJob.getMessage(), takeJob.getMessage());
//        assertEquals(4, redisBlockingDelayQueue.size());

        Thread.sleep(10000);
        long putTimeAgain = System.currentTimeMillis();
        StringDelayedJob putJobAgain = StringDelayedJob.build("testMessageAgain", putTimeAgain + 10000);
        redisBlockingDelayQueue.put(putJobAgain);
        StringDelayedJob takeJobAgain = redisBlockingDelayQueue.take();
        assertTrue(System.currentTimeMillis() - putTimeAgain > 10000);
        assertEquals(putJobAgain.getId(), takeJobAgain.getId());

        redisBlockingDelayQueue.clear();
    }

    @Test
    public void offer() throws InterruptedException {
        redisBlockingDelayQueue.clear();
        long offerTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 10000);
        boolean offer = redisBlockingDelayQueue.offer(putJob);
        if (offer) {
            StringDelayedJob takeJob = redisBlockingDelayQueue.take();
            assertTrue(System.currentTimeMillis() - offerTime > 10000);
            assertEquals(putJob.getMessage(), takeJob.getMessage());
        } else {
            assertEquals(0, redisBlockingDelayQueue.size());
        }
        redisBlockingDelayQueue.offer(putJob);
        if (!redisBlockingDelayQueue.offer(putJob)) {
            assertEquals(1, redisBlockingDelayQueue.size());
        }
    }

    @Test
    public void take() throws InterruptedException {
        redisBlockingDelayQueue.clear();
        long offerTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 10000);
        boolean offer = redisBlockingDelayQueue.offer(putJob);
        if (offer) {
            StringDelayedJob takeJob = redisBlockingDelayQueue.take();
            assertTrue(System.currentTimeMillis() - offerTime > 10000);
            assertEquals(putJob.getMessage(), takeJob.getMessage());
        } else {
            assertEquals(0, redisBlockingDelayQueue.size());
        }
        redisBlockingDelayQueue.offer(putJob);
        if (!redisBlockingDelayQueue.offer(putJob)) {
            assertEquals(1, redisBlockingDelayQueue.size());
        }
    }

    @Test
    public void takeAndRetryConsumer() throws InterruptedException {
        redisBlockingDelayQueue.clear();
        long offerTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 10000);
        boolean offer = redisBlockingDelayQueue.offer(putJob);
        if (offer) {
            try {
                redisBlockingDelayQueue.take((Consumer<StringDelayedJob>) stringDelayedJob -> {
                    throw new RuntimeException("test exception");
                });
            } catch (RuntimeException e) {
                assertEquals("test exception", e.getMessage());
            }
            assertTrue(System.currentTimeMillis() - offerTime > 10000);
        } else {
            assertEquals(0, redisBlockingDelayQueue.size());
        }
        assertNull(redisBlockingDelayQueue.poll());
        Thread.sleep(15000);
        assertEquals(putJob.getId(), redisBlockingDelayQueue.take().getId());
        assertEquals(0, redisBlockingDelayQueue.size());
        redisBlockingDelayQueue.clear();
    }

    @Test
    public void takeAndRetryFunction() {
    }

    @Test
    public void poll() throws InterruptedException {
        redisBlockingDelayQueue.clear();
        long offerTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 5000);
        redisBlockingDelayQueue.offer(putJob);
        System.out.println(System.currentTimeMillis());
        StringDelayedJob poll = redisBlockingDelayQueue.poll();
        System.out.println(System.currentTimeMillis());
        assertNull(poll);
        Thread.sleep(7000);
        StringDelayedJob pollAgain = redisBlockingDelayQueue.poll();
        assertEquals(putJob.getId(), pollAgain.getId());
        redisBlockingDelayQueue.clear();
    }

    @Test
    public void pollAndRetryConsumer() {
    }

    @Test
    public void pollAndRetryFunction() {
    }

    @Test
    public void remainingCapacity() {
    }

    @Test
    public void offer1() {
    }

    @Test
    public void poll1() {
    }

    @Test
    public void peek() throws InterruptedException {
    }


    @Test
    public void remove() {
        redisBlockingDelayQueue.clear();
        long offerTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 5000);
        redisBlockingDelayQueue.put(putJob);
        redisBlockingDelayQueue.remove(putJob);
        assertEquals(0, redisBlockingDelayQueue.size());
        RedisBlockingDelayQueueException exception = null;
        try {
            redisBlockingDelayQueue.remove(putJob);
        } catch (RedisBlockingDelayQueueException e) {
            exception = e;
        }
        assertNotNull(exception);
        redisBlockingDelayQueue.clear();
    }

    @Test
    public void tryRemove() {
        redisBlockingDelayQueue.clear();
        long offerTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 5000);
        redisBlockingDelayQueue.put(putJob);
        assertTrue(redisBlockingDelayQueue.tryRemove(putJob));
        assertFalse(redisBlockingDelayQueue.tryRemove(putJob));
        redisBlockingDelayQueue.clear();
    }

    @Test
    public void completeJob() throws InterruptedException {
        redisBlockingDelayQueue.clear();
        long offerTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 5000);
        redisBlockingDelayQueue.put(putJob);
        RedisBlockingDelayQueueException exception = null;
        try {
            redisBlockingDelayQueue.completeJob(putJob);
        } catch (RedisBlockingDelayQueueException e) {
            exception = e;
        }
        assertNotNull(exception);
        StringDelayedJob takeJob = redisBlockingDelayQueue.take();
        redisBlockingDelayQueue.clear();
    }

    @Test
    public void tryCompleteJob() throws InterruptedException {
        redisBlockingDelayQueue.clear();
        long offerTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 5000);
        redisBlockingDelayQueue.put(putJob);
        assertFalse(redisBlockingDelayQueue.tryCompleteJob(putJob));
        StringDelayedJob takeJob = redisBlockingDelayQueue.take();
        redisBlockingDelayQueue.clear();
    }
}