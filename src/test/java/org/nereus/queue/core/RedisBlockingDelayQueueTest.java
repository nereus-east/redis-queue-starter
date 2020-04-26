package org.nereus.queue.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nereus.queue.RedisConfig;
import org.nereus.queue.exception.RedisBlockingDelayQueueException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@SpringBootApplication(scanBasePackageClasses = RedisConfig.class)
public class RedisBlockingDelayQueueTest {

    @Autowired(required = false)
    RedisBlockingDelayQueue<StringDelayedJob> redisBlockingDelayQueue;

    @Autowired
    StringRedisTemplate stringRedisTemplate;

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

//        Thread.sleep(10000);
        assertEquals(0, redisBlockingDelayQueue.size());
        long putTimeAgain = System.currentTimeMillis();
        StringDelayedJob putJobAgain = StringDelayedJob.build("testMessageAgain", putTimeAgain + 10000);
        redisBlockingDelayQueue.put(putJobAgain);
        StringDelayedJob takeJobAgain = redisBlockingDelayQueue.take();
        assertTrue(System.currentTimeMillis() - putTimeAgain > 10000);
        assertEquals(putJobAgain.getId(), takeJobAgain.getId());

        redisBlockingDelayQueue.clear();
    }

    @Test
    public void putIfAbsent() throws InterruptedException {
        redisBlockingDelayQueue.clear();

        long putTime = System.currentTimeMillis();
        StringDelayedJob putJob = StringDelayedJob.build("testMessage1", putTime + 1000);
        assertTrue(redisBlockingDelayQueue.putIfAbsent(putJob));
        assertFalse(redisBlockingDelayQueue.putIfAbsent(putJob));
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

    @Test
    public void retry() throws InterruptedException {
        redisBlockingDelayQueue.clear();
        //记录Map消费次数
        Map<String, AtomicInteger> jobCloseCountMap = new ConcurrentHashMap<>(20);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                2, 2, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setNameFormat("TestQueue-%d")
                        .setDaemon(true)
                        .build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());
        //构建100个任务,目标100个任务都执行了3次
        threadPoolExecutor.execute(() -> {
            for (int i = 0; i < 100; i++) {
                long offerTime = System.currentTimeMillis();
                StringDelayedJob putJob = StringDelayedJob.build("testMessage1", offerTime + 500);
                redisBlockingDelayQueue.put(putJob);
            }
        });
        threadPoolExecutor.execute(() -> {
            while (true) {
                try {
                    redisBlockingDelayQueue.take((Function<StringDelayedJob, ?>) job -> {
                        jobCloseCountMap.compute(job.getId(), (key, value) -> {
                            if (value != null) {
                                value.incrementAndGet();
                            } else {
                                value = new AtomicInteger(1);
                            }
                            return value;
                        });
                        System.out.println(String.format("job[%s] 即将发生错误", job.getId()));
                        throw new RuntimeException("测试异常重试");
                    });
                } catch (Exception e) {
                    assertEquals("测试异常重试", e.getMessage());
                }
            }
        });
        Thread.sleep(60000);
        for (Map.Entry<String, AtomicInteger> stringAtomicIntegerEntry : jobCloseCountMap.entrySet()) {
            assertEquals(3, stringAtomicIntegerEntry.getValue().get());
        }
        assertEquals(100, jobCloseCountMap.size());
        redisBlockingDelayQueue.clear();
    }

    /** 性能测试配置 */
    private class PerformanceTestConfig {
        /** 生产线程数 */
        private int generateThreadSize;
        /** 每根线程生产多少job */
        private int singleThreadGenerateJobSize;
        /** job生命值/job */
        private int jobLife;
        /** 延迟时间/job */
        private long initDelay;
        /** 正常消费线程数 */
        private int consumerThreadSize;
        /** 异常消费线程数 */
        private int consumerThrowThreadSize;


        /**
         * @param generateThreadSize          生产线程数
         * @param singleThreadGenerateJobSize 每个线程创建的job数
         * @param jobLife                     job生命值
         * @param initDelay                   延迟时间
         * @param consumerThreadSize          正常消费线程数
         * @param consumerThrowThreadSize     异常消费线程数
         */
        public PerformanceTestConfig(int generateThreadSize, int singleThreadGenerateJobSize,
                                     int jobLife, long initDelay,
                                     int consumerThreadSize, int consumerThrowThreadSize) {
            this.generateThreadSize = generateThreadSize;
            this.singleThreadGenerateJobSize = singleThreadGenerateJobSize;
            this.jobLife = jobLife;
            this.initDelay = initDelay;
            this.consumerThreadSize = consumerThreadSize;
            this.consumerThrowThreadSize = consumerThrowThreadSize;
        }

        public int getGenerateThreadSize() {
            return generateThreadSize;
        }

        public void setGenerateThreadSize(int generateThreadSize) {
            this.generateThreadSize = generateThreadSize;
        }

        public int getSingleThreadGenerateJobSize() {
            return singleThreadGenerateJobSize;
        }

        public void setSingleThreadGenerateJobSize(int singleThreadGenerateJobSize) {
            this.singleThreadGenerateJobSize = singleThreadGenerateJobSize;
        }

        public int getJobLife() {
            return jobLife;
        }

        public void setJobLife(int jobLife) {
            this.jobLife = jobLife;
        }

        public long getInitDelay() {
            return initDelay;
        }

        public void setInitDelay(long initDelay) {
            this.initDelay = initDelay;
        }

        public int getConsumerThreadSize() {
            return consumerThreadSize;
        }

        public void setConsumerThreadSize(int consumerThreadSize) {
            this.consumerThreadSize = consumerThreadSize;
        }

        public int getConsumerThrowThreadSize() {
            return consumerThrowThreadSize;
        }

        public void setConsumerThrowThreadSize(int consumerThrowThreadSize) {
            this.consumerThrowThreadSize = consumerThrowThreadSize;
        }

        @Override
        public String toString() {
            return String.format("生产线程数: %s,创建job数/线程: %s, 延迟时间/job: %s, 正常消费线程数: %s, 异常消费线程数: %s",
                    this.generateThreadSize, this.singleThreadGenerateJobSize, this.initDelay, this.consumerThreadSize, this.consumerThrowThreadSize);
        }
    }

    /** 性能测试结果 */
    private class PerformanceTestResult {
        private long generateStartTime;
        private long generateEndTime;
        private long consumerStartTime;
        private long consumerEndTime;

        public long getGenerateTime() {
            return generateEndTime - generateStartTime;
        }

        public long getConsumeTime() {
            return consumerEndTime - consumerStartTime;
        }


        public long getGenerateStartTime() {
            return generateStartTime;
        }

        public void setGenerateStartTime(long generateStartTime) {
            this.generateStartTime = generateStartTime;
        }

        public long getGenerateEndTime() {
            return generateEndTime;
        }

        public void setGenerateEndTime(long generateEndTime) {
            this.generateEndTime = generateEndTime;
        }

        public long getConsumerStartTime() {
            return consumerStartTime;
        }

        public void setConsumerStartTime(long consumerStartTime) {
            this.consumerStartTime = consumerStartTime;
        }

        public long getConsumerEndTime() {
            return consumerEndTime;
        }

        public void setConsumerEndTime(long consumerEndTime) {
            this.consumerEndTime = consumerEndTime;
        }

        @Override
        public String toString() {
            return String.format("生产开始时间: %s,生产结束时间: %s, 生产耗时: %s, 消费开始时间: %s, 消费结束时间: %s, 消费耗时: %s",
                    this.generateStartTime, this.generateEndTime, this.getGenerateTime(), this.consumerStartTime, this.consumerEndTime, this.getConsumeTime());
        }
    }

    @Test
    public void performanceTest() throws InterruptedException {
        //避免多队列互相干扰
//        this.redisBlockingDelayQueue.stop();
        List<PerformanceTestConfig> performanceTestConfigs = new ArrayList<PerformanceTestConfig>() {{
            //仅有成功线程
//            this.add(new PerformanceTestConfig(1, 5000, 3, 100, 1, 0));
//            this.add(new PerformanceTestConfig(4, 5000, 3, 100, 1, 0));
//            this.add(new PerformanceTestConfig(8, 5000, 3, 100, 2, 0));
//            this.add(new PerformanceTestConfig(12, 5000, 3, 100, 3, 0));
//            this.add(new PerformanceTestConfig(16, 5000, 3, 100, 4, 0));
            //仅有失败线程
            this.add(new PerformanceTestConfig(1, 5000, 3, 100, 0, 1));
            this.add(new PerformanceTestConfig(4, 5000, 3, 100, 0, 1));
            this.add(new PerformanceTestConfig(8, 5000, 3, 100, 0, 2));
            this.add(new PerformanceTestConfig(12, 5000, 3, 100, 0, 3));
            this.add(new PerformanceTestConfig(16, 5000, 3, 100, 0, 4));
            this.add(new PerformanceTestConfig(16, 5000, 3, 100, 0, 8));
            this.add(new PerformanceTestConfig(16, 5000, 3, 100, 0, 12));
            this.add(new PerformanceTestConfig(16, 5000, 3, 100, 0, 16));
            //失败+成功
//            this.add(new PerformanceTestConfig(1, 5000, 3, 100, 1, 1));
//            this.add(new PerformanceTestConfig(4, 5000, 3, 100, 1, 1));
//            this.add(new PerformanceTestConfig(8, 5000, 3, 100, 2, 1));
//            this.add(new PerformanceTestConfig(12, 5000, 3, 100, 3, 1));
//            this.add(new PerformanceTestConfig(16, 5000, 3, 100, 4, 1));\
            //失败+成功，延长延迟时间
//            this.add(new PerformanceTestConfig(1, 5000, 3, 5000, 1, 1));
//            this.add(new PerformanceTestConfig(4, 5000, 3, 5000, 1, 1));
//            this.add(new PerformanceTestConfig(8, 5000, 3, 5000, 2, 1));
//            this.add(new PerformanceTestConfig(12, 5000, 3, 5000, 3, 1));
//            this.add(new PerformanceTestConfig(16, 5000, 3, 5000, 4, 1));
        }};
        for (int i = 0; i < performanceTestConfigs.size(); i++) {

            PerformanceTestConfig performanceTestConfig = performanceTestConfigs.get(i);
            System.out.println(String.format("第%s次性能测试开始,参数：%s", i + 1, performanceTestConfig.toString()));
            RedisBlockingDelayQueue<StringDelayedJob> stringDelayedJobs = new RedisBlockingDelayQueue<>(stringRedisTemplate,
                    UUID.randomUUID().toString(),
                    10000,
                    performanceTestConfigs.get(0).getJobLife(),
                    StringDelayedJob.class);
            PerformanceTestResult performanceTestResult = performanceTest(stringDelayedJobs, performanceTestConfig);
//            stringDelayedJobs.stop();
            System.out.println(String.format("第%s次性能测试结束,结果：%s", i + 1, performanceTestResult));
        }
    }

    /** 性能测试 */
    public PerformanceTestResult performanceTest(RedisBlockingDelayQueue queue, PerformanceTestConfig performanceTestConfig) throws InterruptedException {
        queue.clear();
        final long delayMilliseconds = performanceTestConfig.getInitDelay();
        final int singleThreadGenerateJobSize = performanceTestConfig.getSingleThreadGenerateJobSize();
        final int generateThreadSize = performanceTestConfig.getGenerateThreadSize();
        final int consumerThreadSize = performanceTestConfig.getConsumerThreadSize();
        final int consumerThrowThreadSize = performanceTestConfig.getConsumerThrowThreadSize();
        final int allJobCount = singleThreadGenerateJobSize * generateThreadSize;
        final PerformanceTestResult performanceTestResult = new PerformanceTestResult();
        //job消费的时候放入这里，key是job id，value是被执行的次数
        final Map<String, AtomicInteger> jobCloseCountMap = new ConcurrentHashMap<>(allJobCount);
        //等待中的job，job生成的时候，会先放入这里再放入queue
        final Map<String, StringDelayedJob> waitingJob = new ConcurrentHashMap<>(allJobCount);
        //创建线程池
        ThreadPoolExecutor threadPoolExecutor = getPerformanceTestThreadPoolExecutor(generateThreadSize, consumerThreadSize, consumerThrowThreadSize);
        //开始生产job
        performanceTestResult.setGenerateStartTime(System.currentTimeMillis());
        CompletableFuture[] completableFutures = startGenerateJob(queue, delayMilliseconds, singleThreadGenerateJobSize, generateThreadSize, waitingJob, threadPoolExecutor);
        CompletableFuture.allOf(completableFutures).thenAccept(avoid -> {
            performanceTestResult.setGenerateEndTime(System.currentTimeMillis());
        });
        performanceTestResult.setConsumerStartTime(System.currentTimeMillis());
        System.out.println(String.format("当前时间：%s,队列：%s, 开始消费任务", System.currentTimeMillis(), queue.getQueueName()));
        //正常消费job
        startConsumeJob(queue, consumerThreadSize, jobCloseCountMap, waitingJob, threadPoolExecutor);
        //异常消费job
        startConsumeJobException(queue, consumerThrowThreadSize, jobCloseCountMap, waitingJob, threadPoolExecutor);
        //主线程做监视作用,监视任务已经全部被消费并且完成的时间
        while (waitingJob.size() != 0 || jobCloseCountMap.size() < allJobCount) {
//            System.out.println(String.format("当前时间：%s,队列：%s, 已消费[%s]个任务", System.currentTimeMillis(), queue.getQueueName(), jobCloseCountMap.size()));
            Thread.sleep(600);
        }
        performanceTestResult.setConsumerEndTime(System.currentTimeMillis());
        System.out.println(String.format("当前时间：%s,队列：%s, 结束消费,共[%s]个任务,", System.currentTimeMillis(), queue.getQueueName(), allJobCount));
        Thread.sleep(30000);
        return performanceTestResult;
    }


    private void startConsumeJobException(RedisBlockingDelayQueue<StringDelayedJob> redisBlockingDelayQueue, int consumerComplateThreadSize, Map<String, AtomicInteger> jobCloseCountMap, Map<String, StringDelayedJob> waitingJob, ThreadPoolExecutor threadPoolExecutor) {
        //异常消费job
        for (int i = 0; i < consumerComplateThreadSize; i++) {
            threadPoolExecutor.execute(() -> {
                try {
//                    int consumeIndex = 0;
                    while (true) {
                        try {
                            redisBlockingDelayQueue.take((Consumer<StringDelayedJob>) stringDelayedJob ->
                                    this.consumerException(stringDelayedJob, jobCloseCountMap, waitingJob, redisBlockingDelayQueue.getInitLives()));
                        } catch (InterruptedException e) {
                            throw e;
                        } catch (RuntimeException e) {
                            //忽视模拟抛出的异常
                        }
//                        System.out.println(String.format("%s第%s次异常消费结束", Thread.currentThread().getName(), ++consumeIndex));
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(String.format("消费线程:[%s]被中断,测试失败", Thread.currentThread().getName()));
                }
            });
        }
    }

    private void startConsumeJob(RedisBlockingDelayQueue<StringDelayedJob> redisBlockingDelayQueue, int consumerComplateThreadSize, Map<String, AtomicInteger> jobCloseCountMap, Map<String, StringDelayedJob> waitingJob, ThreadPoolExecutor threadPoolExecutor) {
        //正常消费job
        for (int i = 0; i < consumerComplateThreadSize; i++) {
            threadPoolExecutor.execute(() -> {
                try {
//                    int consumeIndex = 0;
                    while (true) {
                        redisBlockingDelayQueue.take((Consumer<StringDelayedJob>) stringDelayedJob ->
                                this.consumer(stringDelayedJob, jobCloseCountMap, waitingJob));
//                        System.out.println(String.format("%s第%s次消费结束", Thread.currentThread().getName(), ++consumeIndex));
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(String.format("消费线程:[%s]被中断,测试失败", Thread.currentThread().getName()));
                }
            });
        }
    }

    /**
     * 生产job
     *
     * @param delayMilliseconds           延迟时间
     * @param redisBlockingDelayQueue     生产的job放入哪个队列
     * @param singleThreadGenerateJobSize 每个线程创建多少个job
     * @param generateThreadSize          启用多少个线程去创建job
     * @param waitingJob                  创建的job都会放在这里
     * @param threadPoolExecutor          去做这件事情的线程池
     */
    private CompletableFuture<Void>[] startGenerateJob(RedisBlockingDelayQueue<StringDelayedJob> redisBlockingDelayQueue, long delayMilliseconds,
                                                       int singleThreadGenerateJobSize, int generateThreadSize, Map<String, StringDelayedJob> waitingJob, ThreadPoolExecutor threadPoolExecutor) {
        CompletableFuture<Void>[] futures = new CompletableFuture[generateThreadSize];
        //生产job
        for (int i = 0; i < generateThreadSize; i++) {
            int finalIndex = i;
            futures[i] = CompletableFuture.runAsync(() -> {
                        generateJobToQueue(waitingJob, redisBlockingDelayQueue, finalIndex * singleThreadGenerateJobSize, (finalIndex + 1) * singleThreadGenerateJobSize - 1, delayMilliseconds);
                    },
                    threadPoolExecutor);
        }
        return futures;
    }

    private ThreadPoolExecutor getPerformanceTestThreadPoolExecutor(int generateThreadSize, int consumerComplateThreadSize, int consumerThrowThreadSize) {
        //根据生产、正常消费、异常消费线程数构建线程池
        return new ThreadPoolExecutor(
                generateThreadSize + consumerComplateThreadSize + consumerThrowThreadSize,
                generateThreadSize + consumerComplateThreadSize + consumerThrowThreadSize,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setNameFormat("TestQueue-%d")
                        .setDaemon(true)
                        .build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    private void generateJobToQueue(Map<String, StringDelayedJob> waitingJob, RedisBlockingDelayQueue<StringDelayedJob> queue, int start, int end, long delayMilliseconds) {
        System.out.println(String.format("当前时间：%s,队列：%s, 开始创建任务,start：[%s],end：[%s]", System.currentTimeMillis(), queue.getQueueName(), start, end));
        int i = start;
        while (i <= end) {
            StringDelayedJob job = StringDelayedJob.build(
                    String.format("this is the %s-%s job", queue.getQueueName(), ++i),
                    String.format("%s-%s", queue.getQueueName(), i),
                    System.currentTimeMillis() + delayMilliseconds);
            waitingJob.put(job.getId(), job);
            queue.put(job);
        }
        System.out.println(String.format("当前时间：%s,队列：%s, 结束创建任务,start：[%s],end：[%s]", System.currentTimeMillis(), queue.getQueueName(), start, end));
    }

    private void consumer(StringDelayedJob stringDelayedJob, Map<String, AtomicInteger> jobCloseCountMap, Map<String, StringDelayedJob> waitingJob) {
        jobCloseCountMap.compute(stringDelayedJob.getId(), (s, atomicInteger) -> {
            if (atomicInteger == null) {
                atomicInteger = new AtomicInteger(0);
            }
            atomicInteger.incrementAndGet();
            waitingJob.remove(s);
            return atomicInteger;
        });
    }

    private void consumerException(StringDelayedJob stringDelayedJob,
                                   Map<String, AtomicInteger> jobCloseCountMap,
                                   Map<String, StringDelayedJob> waitingJob,
                                   int jobLives) throws RuntimeException {
        jobCloseCountMap.compute(stringDelayedJob.getId(), (s, atomicInteger) -> {
            if (atomicInteger == null) {
                atomicInteger = new AtomicInteger(0);
            }
            int consumerCount = atomicInteger.incrementAndGet();
            if (consumerCount == jobLives) {
                waitingJob.remove(s);
            }
            return atomicInteger;
        });
        throw new RuntimeException();
    }

    /**
     * 连接池测试
     */
    @Test
    public void connectionPoolTest() throws InterruptedException {
        RedisConnectionFactory connectionFactory = stringRedisTemplate.getConnectionFactory();
        //队列数量
        int queueCount = 1;
        //消费线程数/每个队列
        int consumeThreadSize = 8;
        //生产线程数/每个队列
        int productThreadSize = 8;
        int singleThreadGenerateJobSize = 1000;
        int allJobCount = singleThreadGenerateJobSize * productThreadSize * queueCount;
        int delayMilliseconds = 5000;
        HashMap<String, RedisBlockingDelayQueue> queueHashMap = new HashMap<>(queueCount);
        ThreadPoolExecutor threadPoolExecutor = getConnectionPoolTestThreadPoolExecutor(queueCount, consumeThreadSize, productThreadSize);
        Map<String, AtomicInteger> jobCloseCountMap = new ConcurrentHashMap<>(allJobCount);
        Map<String, StringDelayedJob> waitingJob = new ConcurrentHashMap<>(allJobCount);
        for (int i = 0; i < queueCount; i++) {
            RedisBlockingDelayQueue<StringDelayedJob> stringDelayedJobs =
                    new RedisBlockingDelayQueue<>(stringRedisTemplate, String.format("QUEUE-%s", i), StringDelayedJob.class);
            stringDelayedJobs.clear();
            queueHashMap.put(stringDelayedJobs.getQueueName(), stringDelayedJobs);
            //开始生产job
            startGenerateJob(stringDelayedJobs, delayMilliseconds, singleThreadGenerateJobSize, productThreadSize, waitingJob, threadPoolExecutor);
            System.out.println(String.format("当前时间：%s,队列：%s, 开始消费任务", System.currentTimeMillis(), stringDelayedJobs.getQueueName()));
            //正常消费job
            startConsumeJob(stringDelayedJobs, consumeThreadSize, jobCloseCountMap, waitingJob, threadPoolExecutor);

        }
        //主线程做监视作用,监视任务已经全部被消费并且完成的时间
        while (waitingJob.size() != 0 || jobCloseCountMap.size() < allJobCount) {
//            System.out.println(String.format("当前时间：%s,队列：%s, 已消费[%s]个任务", System.currentTimeMillis(), jobCloseCountMap.size()));
            Thread.sleep(10);
        }
        System.out.println(String.format("当前时间：%s,结束消费,共[%s]个任务,", System.currentTimeMillis(), allJobCount));
    }

    private ThreadPoolExecutor getConnectionPoolTestThreadPoolExecutor(int queueCount, int consumerThreadSize, int productThreadSize) {
        //根据队列数量,消费者、生产者数构建线程池
        return new ThreadPoolExecutor(
                queueCount * (consumerThreadSize + productThreadSize),
                queueCount * (consumerThreadSize + productThreadSize),
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setNameFormat("TestQueue-%d")
                        .setDaemon(true)
                        .build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());
    }
}