package org.nereus.queue.core;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.nereus.queue.exception.RedisBlockingDelayQueueException;
import org.nereus.queue.exception.RedisHashOpsException;
import org.nereus.queue.exception.RedisZSetOpsException;
import org.nereus.queue.helper.HashCompareSetOnListMoveToSortedSetParam;
import org.nereus.queue.helper.RedisScriptExecuteHelper;
import org.nereus.queue.helper.SortedSetAndHashRemoveResult;
import org.nereus.queue.constant.RedisBlockingDelayQueueConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @description: Delay blocking queue based on redis
 * @author: nereus east
 * @data: 2020/3/19 15:47
 */
public class RedisBlockingDelayQueue<E extends AbstractDelayedJob> extends AbstractQueue<E>
        implements BlockingQueue<E> {

    private static final long DEFAULT_TIMEOUT = 1000;

    private static final int DEFAULT_LIVES = 3;

    private volatile boolean runListen = false;
    private volatile boolean runBulkPop = false;
    private volatile boolean runTransport = false;
    private volatile boolean runGuaranteed = false;
    private volatile boolean runRetry = false;

    private final Logger log = LoggerFactory.getLogger(RedisBlockingDelayQueue.class);

    private final ThreadPoolExecutor queueExecutor;

    /**
     * Lock held by listen, bulkPop
     */
    private final ReentrantLock takeLock = new ReentrantLock();

    private final Condition empty = takeLock.newCondition();
    private final Condition notEmpty = takeLock.newCondition();

    /**
     * Lock held by put, offer, etc
     */
    private final ReentrantLock transferLock = new ReentrantLock();

    private final Condition expire = transferLock.newCondition();
    private final Condition poll = transferLock.newCondition();

    /**
     * Lock held by retry
     */
    private final ReentrantLock retryLock = new ReentrantLock();

    private final Condition backEmpty = retryLock.newCondition();


    private final AtomicLong nextTransportTime = new AtomicLong(System.currentTimeMillis());
    private final String KEY_WAITING;
    private final String KEY_CONTENT;
    private final String KEY_READY;
    private final String KEY_BACK;
    private final List<String> ALL_KEY;
    /**
     * Java Generics - Type Erasure
     */
    private final Class<E> eClass;

    StringRedisTemplate redisTemplate;

    protected final String queueName;

    /**
     * Execution timeout in milliseconds
     */
    private final long timeout;

    /**
     * Initial health of each job
     */
    private final int initLives;

    protected final int capacity;

    /**
     * The remaining execution time in the internal queue when the service is stopped
     */
    private final long internalRemainingTimeOnShutdown = 20000L;

    /**
     * When the expected time is up, the job will be put into this queue
     */
    private BlockingQueue<E> internalConsumptionQueue = new LinkedBlockingDeque<>();


    public RedisBlockingDelayQueue(StringRedisTemplate redisTemplate, String queueName, Class<E> eClass) {
        this(Integer.MAX_VALUE, redisTemplate, queueName, DEFAULT_TIMEOUT, DEFAULT_LIVES, eClass);
    }


    public RedisBlockingDelayQueue(StringRedisTemplate redisTemplate, String queueName,
                                   long timeout, int initLives, Class<E> eClass) {
        this(Integer.MAX_VALUE, redisTemplate, queueName, timeout, initLives, eClass);
    }

    /**
     * @Description: todo implement capacity limit
     * todo some of the features that require distributed locking are not yet implemented
     * @Author nereus east
     * @Date 2020/3/23 17:05
     **/
    private RedisBlockingDelayQueue(int capacity, StringRedisTemplate redisTemplate, String queueName,
                                    long timeout, int initLives, Class<E> eClass) {
        constructorParamsValidation(capacity, redisTemplate, queueName, eClass);
        this.capacity = capacity;
        this.redisTemplate = redisTemplate;
        this.queueName = queueName;
        this.timeout = timeout;
        this.initLives = initLives;
        this.KEY_WAITING = queueName + RedisBlockingDelayQueueConstant.KEY_PRE_WAITING_SORTED_SET;
        this.KEY_CONTENT = queueName + RedisBlockingDelayQueueConstant.KEY_PRE_CONTENT_HASH;
        this.KEY_READY = queueName + RedisBlockingDelayQueueConstant.KEY_PRE_READY_LIST;
        this.KEY_BACK = queueName + RedisBlockingDelayQueueConstant.KEY_PRE_BACK_LIST;
        this.ALL_KEY = new ArrayList<String>(4) {{
            this.add(KEY_WAITING);
            this.add(KEY_CONTENT);
            this.add(KEY_READY);
            this.add(KEY_BACK);
        }};
        this.eClass = eClass;
        this.queueExecutor = new ThreadPoolExecutor(
                5, 5, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setNameFormat("RedisBlockingDelayQueue-%d")
                        .setDaemon(true)
                        .build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());
        this.init();
        this.registerShutdownEvent();
    }

    private void constructorParamsValidation(int capacity, StringRedisTemplate redisTemplate, String queueName, Class<E> eClass) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("RedisBlockingDelayQueue init fail, queue length cannot be negative.");
        }
        Assert.notNull(redisTemplate, "RedisBlockingDelayQueue init fail, create queueRedisTemplate first.");
        Assert.hasText(queueName, "RedisBlockingDelayQueue init fail, please specify the queueName.");
        Assert.notNull(eClass, "RedisBlockingDelayQueue init fail, please specify the type of queue job.");
    }

    private void runAll() {
        this.runListen = true;
        this.runBulkPop = true;
        this.runTransport = true;
        this.runGuaranteed = true;
        this.runRetry = true;
    }

    private void stopAll() {
        this.runListen = false;
        this.runBulkPop = false;
        this.runTransport = false;
        this.runGuaranteed = false;
        this.runRetry = false;
    }

    /**
     * @Description: 1: Using the listener thread, get the job through the redis RPOPLPUSH command
     * 2: Start the transport thread
     * 3: Start the guaranteed thread
     * @Author nereus east
     * @Date 2020/3/22 16:00
     **/
    private void init() {
        this.runAll();
        this.queueExecutor.execute(() -> {
            try {
                listen();
            } catch (InterruptedException e) {
                log.info("Queue: [{}], the listen thread is interrupted and will stop working!", this.queueName);
            }
        });
        this.queueExecutor.execute(() -> {
            try {
                bulkPop();
            } catch (InterruptedException e) {
                log.info("Queue: [{}], the bulkPop thread is interrupted and will stop working!", this.queueName);
            }
        });
        this.queueExecutor.execute(() -> {
            try {
                transportDelay();
            } catch (InterruptedException e) {
                log.info("Queue: [{}], the transport thread is interrupted and will stop working!", this.queueName);
            }
        });
        this.queueExecutor.execute(() -> {
            try {
                guaranteed();
            } catch (InterruptedException e) {
                log.info("Queue: [{}], the guaranteed thread is interrupted and will stop working!", this.queueName);
            }
        });
        if (this.timeout > 0 && this.initLives > 1) {
            this.queueExecutor.execute(() -> {
                try {
                    runRetryDetection();
                } catch (InterruptedException e) {
                    log.info("Queue: [{}], the retry thread is interrupted and will stop working!", this.queueName);
                }
            });
        } else {
            log.info("Queue: [{}], config: {timeout: {}, initLives: {}}, retry detection will not be enabled", this.queueName);
        }
    }

    private void putToInternalAndUpdateMetadata(E job) throws InterruptedException {
        job.setRealTime(System.currentTimeMillis());
        redisTemplate.opsForHash().put(KEY_CONTENT, job.getId(), serialize(job));
        internalConsumptionQueue.put(job);
    }

    /**
     * A single thread listens for redis and starts a batch thread to fetch data when a job expires
     *
     * @throws InterruptedException
     */
    private void listen() throws InterruptedException {
        while (this.runListen) {
            takeLock.lockInterruptibly();
            try {
                String jobId = redisTemplate.opsForList().rightPopAndLeftPush(KEY_READY, KEY_BACK, 0, TimeUnit.MILLISECONDS);
                E job = getContentById(jobId);
                putToInternalAndUpdateMetadata(job);
                notEmpty.signalAll();
                empty.await();
            } finally {
                takeLock.unlock();
            }
        }
    }

    private final int bulkSize = 100;

    private void bulkPop() throws InterruptedException {
        while (this.runBulkPop) {
            takeLock.lockInterruptibly();
            try {
                while (true) {
                    List<String> jobIdList = RedisScriptExecuteHelper.listRightPopLeftPushAndBulkGet(redisTemplate, KEY_READY, KEY_BACK, bulkSize);
                    if (jobIdList != null && jobIdList.size() > 0) {
                        consumerContentByIds(jobIdList, job -> {
                            try {
                                putToInternalAndUpdateMetadata(job);
                            } catch (InterruptedException e) {
                                log.error("Queue: [{}], bulk pop error! Can't roll back temporarily, wait for retry detection and re-consumption", this.queueName, e);
                            }
                        });
                    }
                    if (jobIdList == null || jobIdList.size() < bulkSize) {
                        empty.signalAll();
                        break;
                    }
                }
                notEmpty.await();
            } finally {
                takeLock.unlock();
            }
        }
    }

    private void guaranteed() throws InterruptedException {
        while (this.runGuaranteed) {
            transferLock.lockInterruptibly();
            try {
                this.nextTransportTime.set(0);
                expire.signalAll();
                poll.await(1, TimeUnit.SECONDS);
            } finally {
                transferLock.unlock();
            }
        }
    }

    private static final int FACTOR = 2;
    private AtomicLong retryBatchSize = new AtomicLong(1);

    private void runRetryDetection() throws InterruptedException {
        while (this.runRetry) {
            retryLock.lockInterruptibly();
            try {
                List<String> jobContentStrList = RedisScriptExecuteHelper.listWithHashMGet(redisTemplate, KEY_BACK, KEY_CONTENT, -retryBatchSize.get(), -1, true);
                if (jobContentStrList.size() == 0) {
                    backEmpty.await(1, TimeUnit.SECONDS);
                    retryBatchSize.set(1);
                    continue;
                }
                RetryResult retryResult = retryDetection(jobContentStrList, retryBatchSize.get());
                long planCount = retryBatchSize.getAndUpdate(operand -> operand == retryResult.successCount ? operand * FACTOR : 1);
                if (planCount > retryResult.successCount) {
                    final E firstNoRetry = retryResult.firstNoRetry;
                    long awaitBaseTime = firstNoRetry.getRealTime() > 0 ? firstNoRetry.getRealTime() : firstNoRetry.getExpectedTime();
                    backEmpty.await(awaitBaseTime + timeout - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                }
            } finally {
                retryLock.unlock();
            }
        }
    }

    private class RetryResult {
        private long successCount;
        private E firstNoRetry;

        public RetryResult(long successCount, E firstNoRetry) {
            this.successCount = successCount;
            this.firstNoRetry = firstNoRetry;
        }
    }

    /**
     * @param jobContentStrList
     * @return Number moved to waiting queue
     */
    private RetryResult retryDetection(List<String> jobContentStrList, long retryBatchSize) {
        long currentTimeMillis = System.currentTimeMillis();
        /** These are only possible retry tasks, and the actual results of the lua script execution shall prevail */
//        List<E> tryRetryJobList = new ArrayList<>(jobContentStrList.size());
        List<HashCompareSetOnListMoveToSortedSetParam> params = new ArrayList<>(jobContentStrList.size());
        E firstNoRetry = null;
        for (String jobContent : jobContentStrList) {
            E job = deserialize(jobContent);
            if (job.loseOneLifeAndGet() > 0 && job.getRealTime() - currentTimeMillis > this.timeout) {
                params.add(
                        HashCompareSetOnListMoveToSortedSetParam
                                .build(job.getId(), jobContent, serialize(job), job.getExpectedTime()));
            } else {
                //In the case of the same timeout, the previous job has not timed out, then the next job must not have timed out
                firstNoRetry = job;
                break;
            }
        }
        if (CollectionUtils.isEmpty(params)) {
            return new RetryResult(0, firstNoRetry);
        }
        long retryCount = RedisScriptExecuteHelper.hashCompareSetOnListMoveToSortedSet(redisTemplate, KEY_BACK, KEY_WAITING, KEY_CONTENT, params, retryBatchSize);
        return new RetryResult(retryCount, firstNoRetry);
    }

    private boolean retry(E job) {
        long currentTimeMillis = System.currentTimeMillis();
        retryLock.lock();
        try {
            String jobContent = serialize(job);
            if (job.loseOneLifeAndGet() > 0) {
                ArrayList<HashCompareSetOnListMoveToSortedSetParam> scriptParams = new ArrayList<HashCompareSetOnListMoveToSortedSetParam>() {
                    {
                        this.add(HashCompareSetOnListMoveToSortedSetParam
                                .build(job.getId(), jobContent, serialize(job), job.getExpectedTime()));
                    }
                };
                return RedisScriptExecuteHelper
                        .hashCompareSetOnListMoveToSortedSet(redisTemplate, KEY_BACK, KEY_WAITING, KEY_CONTENT, scriptParams
                                , 100) > 0;
            }
        } finally {
            retryLock.unlock();
        }
        return false;
    }

    /**
     * @Description: implement elegant shutdown
     * todo implements
     * @Author nereus east
     * @Date 2020/3/22 20:24
     **/
    private void registerShutdownEvent() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop();
            } catch (InterruptedException e) {
                log.error("Graceful shutdown thread is interrupted, data may be lost", e);
            }
        }));
    }

    private void stop() throws InterruptedException {
        long currentTimeMillis = System.currentTimeMillis();
        this.stopAll();
        queueExecutor.shutdown();
        queueExecutor.awaitTermination(5, TimeUnit.SECONDS);
        queueExecutor.shutdownNow();
        while (System.currentTimeMillis() - currentTimeMillis < internalRemainingTimeOnShutdown
                && internalConsumptionQueue.size() > 0) {
            Thread.sleep(100);
        }
    }

    /**
     * @Description: Move the job from the waiting set to the ready list
     * @Author nereus east
     * @Date 2020/3/22 20:51
     **/
    private void transportDelay() throws InterruptedException {
        while (this.runTransport) {
            transferLock.lockInterruptibly();
            try {
                if (nextTransportTime.get() > System.currentTimeMillis()) {
                    expire.await(nextTransportTime.get() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                }
                long currentTransportTime = nextTransportTime.get();
                if (currentTransportTime < System.currentTimeMillis()) {
                    transport();
                    refreshNextTransportTime(currentTransportTime);
                }
            } finally {
                transferLock.unlock();
            }
        }

    }

    private long transport() {
        return RedisScriptExecuteHelper.sortedSetToList(redisTemplate, KEY_WAITING, KEY_READY, 0, System.currentTimeMillis());
    }

    /**
     * @Description:
     * @Author nereus east
     * @Date 2020/3/23 14:32
     **/
    private void refreshNextTransportTime(long currentTransportTime) {
        Set<ZSetOperations.TypedTuple<String>> typedTuples = redisTemplate.opsForZSet().rangeWithScores(this.KEY_WAITING, 0, 0);
        if (typedTuples.isEmpty()) {
            this.nextTransportTime.compareAndSet(currentTransportTime, Long.MAX_VALUE);
        } else {
            long newNextTime = ((ZSetOperations.TypedTuple<String>) (typedTuples.toArray()[0])).getScore().longValue();
            this.nextTransportTime.compareAndSet(currentTransportTime, newNextTime);
        }
    }

    private void updateNextTransportTime(long nextTransportTime) {
        transferLock.lock();
        try {
            long oldNext = this.nextTransportTime.getAndUpdate(operand -> operand > nextTransportTime ? nextTransportTime : operand);
//            this.nextTransportTime.updateAndGet()
            if (nextTransportTime < oldNext) {
                expire.signalAll();
            }
        } finally {
            transferLock.unlock();
        }
    }

    private E getContentById(String jobId) {
        String jobContent = redisTemplate.<String, String>opsForHash().get(KEY_CONTENT, jobId);
        return deserialize(jobContent);
    }

    private void consumerContentByIds(List<String> jobIdList, Consumer<E> jobConsumer) throws InterruptedException {
        List<String> jobContentStrList = redisTemplate.<String, String>opsForHash().multiGet(KEY_CONTENT, jobIdList);
        for (String contentStr : jobContentStrList) {
            E job = deserialize(contentStr);
            jobConsumer.accept(job);
        }
    }

    private void executeJobConsumer(Consumer<E> eConsumer, E job) {
        long currentTimeMillis = System.currentTimeMillis();
        try {
            eConsumer.accept(job);
            this.completeJob(job);
        } catch (Exception e) {
            if (System.currentTimeMillis() - currentTimeMillis < this.timeout) {
                boolean retry = retry(job);
                log.debug("job id [{}], execute exception, retry {}", job.getId(), retry ? "success" : "error");
            }
            throw e;
        }
    }

    private Object executeJobFunction(Function<E, ?> eFunction, E job) {
        long currentTimeMillis = System.currentTimeMillis();
        try {
            Object result = eFunction.apply(job);
            this.completeJob(job);
            return result;
        } catch (Exception e) {
            if (System.currentTimeMillis() - currentTimeMillis < this.timeout) {
                boolean retry = retry(job);
                log.debug("job id [{}], execute exception, retry {}", job.getId(), retry ? "success" : "error");
            }
            throw e;
        }
    }

    private E deserialize(String jobStr) {
        return JSON.parseObject(jobStr, eClass, Feature.SupportNonPublicField);
    }

    private String serialize(E job) {
        return JSON.toJSONString(job);
    }

    @Deprecated
    @Override
    public Iterator<E> iterator() {
        throw new RedisBlockingDelayQueueException("Not yet implemented, looking forward to the next release");
    }

    @Override
    public int size() {
//        return RedisScriptExecuteHelper.sortedSetAndListCount(this.redisTemplate, this.KEY_WAITING, this.KEY_READY);
        return Math.toIntExact(redisTemplate.opsForHash().size(this.KEY_CONTENT));
    }

    @Override
    public void put(E e) throws RedisBlockingDelayQueueException {
        Assert.notNull(e, "Job must be not null");
        initJob(e);
        String id = e.getId();
        String content = serialize(e);
        try {
            RedisScriptExecuteHelper.sortedSetAndHashAdd(redisTemplate, KEY_WAITING, KEY_CONTENT, id, content, (double) e.getExpectedTime());
            log.debug("Job id[{}] put success!");
        } catch (RedisHashOpsException ex) {
            log.warn("Job id[{}] data corruption, Make sure that the name are unique!", id);
            throw new RedisBlockingDelayQueueException(
                    String.format("Job id[%s] is exist, if you need to update the job content, call the update method!", id));
        } catch (RedisZSetOpsException ex) {
            log.warn(String.format("Job id[%s] data corruption, Make sure that the name are unique!"));
        }
        updateNextTransportTime(e.getExpectedTime());
    }

    private void initJob(E e) {
        e.setTopic(this.queueName);
        e.setLives(this.initLives);
        e.setTimeout(this.timeout);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        Assert.notNull(e, "Job must be not null");
        try {
            this.put(e);
        } catch (RedisBlockingDelayQueueException ex) {
            return false;
        }
        return true;
    }

    /**
     * PS : This method does not support retrying the job, as it is unclear whether the caller will use the complete method
     *
     * @return
     * @throws InterruptedException
     */
    @Deprecated
    @Override
    public E take() throws InterruptedException {
        E job = this.internalConsumptionQueue.take();
        this.completeJob(job);
        return job;
    }

    public void take(Consumer<E> eConsumer) throws InterruptedException {
        E job = this.internalConsumptionQueue.take();
        executeJobConsumer(eConsumer, job);
    }

    public Object take(Function<E, ?> eFunction) throws InterruptedException {
        E job = this.internalConsumptionQueue.take();
        return executeJobFunction(eFunction, job);
    }

    /**
     * PS : This method does not support retrying the job, as it is unclear whether the caller will use the complete method
     *
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    @Deprecated
    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E job = internalConsumptionQueue.poll(timeout, unit);
        if (job != null) {
            this.completeJob(job);
        }
        return job;
    }

    public void poll(Consumer<E> eConsumer, long timeout, TimeUnit unit) throws InterruptedException {
        E job = internalConsumptionQueue.poll(timeout, unit);
        if (job == null) {
            return;
        }
        executeJobConsumer(eConsumer, job);
    }

    public Object poll(Function<E, ?> eFunction, long timeout, TimeUnit unit) throws InterruptedException {
        E job = internalConsumptionQueue.poll(timeout, unit);
        if (job == null) {
            return null;
        }
        return executeJobFunction(eFunction, job);
    }

    @Override
    public int remainingCapacity() {
        return capacity - this.size();
    }

    @Deprecated
    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Deprecated
    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new RedisBlockingDelayQueueException("Not yet implemented, looking forward to the next release");
    }

    @Override
    public boolean offer(E e) {
        Assert.notNull(e, "Job must be not null");
        try {
            this.put(e);
        } catch (RedisBlockingDelayQueueException ex) {
            return false;
        }
        return true;
    }

    /**
     * PS : This method does not support retrying the job, as it is unclear whether the caller will use the complete method
     *
     * @return
     */
    @Deprecated
    @Override
    public E poll() {
        String jobId = redisTemplate.opsForList().rightPopAndLeftPush(KEY_READY, KEY_BACK);
        if (!StringUtils.isEmpty(jobId)) {
            return getContentById(jobId);
        }
        E poll = internalConsumptionQueue.poll();
        if (poll != null) {
            completeJob(poll);
        }
        return poll;
    }

    @Deprecated
    @Override
    public E peek() {
        throw new RedisBlockingDelayQueueException("Not yet implemented, looking forward to the next release");
//        String first = RedisScriptExecuteHelper.sortedFirstWithHashGet(redisTemplate, KEY_WAITING, KEY_CONTENT);
//        return StringUtils.isEmpty(first) ? null : deserialize(first);
    }

    /**
     * AbstractQueue cleans up elements through poll, which can only clean up expired jobs
     */
    @Override
    public void clear() {
        redisTemplate.delete(ALL_KEY);
        internalConsumptionQueue.clear();
    }

    public void remove(E e) throws RedisBlockingDelayQueueException {
        Assert.notNull(e, "Job must be not null");
        remove(e.getId());
    }

    /**
     * @Description: A ready job cannot be removed because it will traverse the redis list.
     *  So only remove jobs that have not expired
     * @Author nereus east
     * @Date 2020/3/25 16:13
     **/
    public void remove(String id) throws RedisBlockingDelayQueueException {
        Assert.notNull(id, "Job id must be not null");
        SortedSetAndHashRemoveResult sortedSetAndHashRemoveResult
                = RedisScriptExecuteHelper.sortedSetAndHashRemove(this.redisTemplate, KEY_WAITING, KEY_CONTENT, id);
        if (sortedSetAndHashRemoveResult.getSortedRemoveCount() == 0) {
            throw new RedisBlockingDelayQueueException(String.format("Job %s not found, may have been consumed", id));
        }
    }

    public boolean tryRemove(E e) {
        Assert.notNull(e, "Job must be not null");
        return tryRemove(e.getId());
    }

    private boolean tryRemove(String id) {
        Assert.notNull(id, "Job id must be not null");
        SortedSetAndHashRemoveResult sortedSetAndHashRemoveResult
                = RedisScriptExecuteHelper.sortedSetAndHashRemove(this.redisTemplate, KEY_WAITING, KEY_CONTENT, id);
        return sortedSetAndHashRemoveResult.getSortedRemoveCount() > 0;
    }

    public void completeJob(E e) throws RedisBlockingDelayQueueException {
        completeJob(e.getId());
    }

    /**
     * The current time complexity: O(N)
     * todo performance optimization
     *
     * @param id
     * @throws RedisBlockingDelayQueueException
     */
    public void completeJob(String id) throws RedisBlockingDelayQueueException {
        if (!RedisScriptExecuteHelper.listAndHashRemove(redisTemplate, KEY_BACK, KEY_CONTENT, id)) {
            throw new RedisBlockingDelayQueueException(String.format("Job %s not found, may have not been consumed", id));
        }
    }

    public boolean tryCompleteJob(E e) throws RedisBlockingDelayQueueException {
        return tryCompleteJob(e.getId());
    }

    public boolean tryCompleteJob(String id) throws RedisBlockingDelayQueueException {
        return RedisScriptExecuteHelper.listAndHashRemove(redisTemplate, KEY_BACK, KEY_CONTENT, id);
    }
}
