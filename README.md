##<center> RedisBlockingDelayQueue 使用说明 </center>


### 基本原理、说明、Q&A
* 队列基于Redis实现，所以接入需要一个StringRedisTemplate

* 每个队列需要有一个 **QueueName**，通过构造函数中传入，也可以理解为Topic，用于在Redis中的Key的隔离

* 一期主要实现了 **BlockingQueue<E>** 接口，使用时类似于 **new DelayQueue<E>()** ，当然， **E extends Delayed**

* 部分需要分布式锁的功能在一期出于性能考虑并未实现（例如容量限制）

* 正常情况，不考虑业务处理，TPS在个人开发电脑可以达到2000+

* 队列中存放的对象需要继承于AbstractDelayedJob，可通过重写computeRetryInterval，自定义重试间隔的计算策略

* 因为 **BlockingQueue** 默认是没有考虑重试机制的，所以部分方法在 **RedisBlockingDelayQueue** 中不建议使用，例如原生的 **take()** 方法

``` java
    Job job = queue.take();
    doSomething(job);
```
      方法，建议使用的方法如下：
``` java
    //该方法会进行异常处理，并根据配置的重试次数重新加入队列：
    queue.take(job -> doSomething(job));
    //或者
    Result result = queue.take(job -> doSomething(job));
```

* 因为很多操作实现基于Redis的Lua脚本，所以目前暂不支持cluster模式

* 阻塞通过一个单独的线程调用Redis 中 **List** 的 **BRPOPLPUSH** 实现，所以延迟有较高的时效性，同时避免了轮询

* 延迟通过Redis 中 **ZSet** 的 **Score** 实现

* 设计文档见：[基于 redis 的延迟、阻塞队列](https://nereus-east.github.io/2020/04/05/ji-yu-redis-de-yan-chi-zu-sai-dui-lie/)

### 如何接入
1. 引入Maven依赖，目前1.0.0

```
        <dependency>
            <groupId>org.nereus.queue</groupId>
            <artifactId>redis-queue-starter</artifactId>
            <version>1.0.0</version>
        </dependency>
```

2. 想好自己的队列中，需要放入什么内容，想好之后写一个类继承于 **AbstractDelayedJob** ，默认提供了一个 **StringDelayedJob** ，里面只有一个 **String** 类型的字段，名为 **message**

3. 准备好一个StringRedisTemplate

4. 给自己的队列取一个名字，比如: MYSELF-QUEUE

5. 配置一个RedisBlockingDelayQueue

6. 简版见代码，

``` java
    /**
     * @Description:
     * 队列中要求对象都集成于AbstractDelayedJob
     * 可以自定义业务类，进行自定义的字段扩展
     */
    public class StringDelayedJob extends AbstractDelayedJob {
        private String message;

        public static StringDelayedJob build(String message, long expectedTime) {
            return new StringDelayedJob(message, expectedTime);
        }

        public StringDelayedJob() {
        }

        public StringDelayedJob(String message, long expectedTime) {
            super(expectedTime);
            this.message = message;
        }

        public String getMessage() {
            return this.message;
        }
    }
```

``` java
    @Configuration
    public class RedisQueueConfiguration {

        @Bean("queueRedisTemplate")
        public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory){
            StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
            stringRedisTemplate.setConnectionFactory(redisConnectionFactory);
            stringRedisTemplate.setDefaultSerializer(new StringRedisSerializer());
            return stringRedisTemplate;
        }

        /**
         * 配置一个 RedisBlockingDelayQueue 实例
         * MYSELF-QUEUE 是我们给自己的队列取的名字，也可以理解为Topic
         * ImportantDelayJob.class 是队列中放的对象
         * 默认的执行超时时间为：10000Ms，即10秒
         * 默认的最多执行次数为：3次，即最多重复两次
         * @param queueRedisTemplate 需要一个StringRedisTemplate
         * @return 返回配置好的实例
         */
        @Bean
        public RedisBlockingDelayQueue<StringDelayedJob> redisBlockingDelayQueue(
                @Qualifier("queueRedisTemplate") StringRedisTemplate queueRedisTemplate) {
            return new RedisBlockingDelayQueue<>(queueRedisTemplate,
                    "MYSELF-QUEUE",
                    StringDelayedJob.class);
        }

    }
```

``` java

    @Service
    public class TestService {

        @Autowired
        private RedisBlockingDelayQueue<StringDelayedJob> redisBlockingDelayQueue;

        public void test() throws InterruptedException{
            //系统当前时间
            long offerTime = System.currentTimeMillis();
            //创建一个job，希望在当前时间的五秒过后才能被消费者消费
            StringDelayedJob putJob = StringDelayedJob.build("testMessage", offerTime + 5000);
            //把job放入队列中
            redisBlockingDelayQueue.put(putJob);
            //消费队列中的job，目的是打印job中的message
            redisBlockingDelayQueue.take((Consumer<StringDelayedJob>) stringDelayedJob -> {
                System.out.println(stringDelayedJob.getMessage());
            });
        }
    }
```

7. 详细版，主要在构造参数上的不同，可以自定义job执行的超时时间以及重试的次数
``` java

    @Bean
    public RedisBlockingDelayQueue<ImportantDelayJob> redisBlockingDelayQueue(
            @Qualifier("queueRedisTemplate") StringRedisTemplate queueRedisTemplate) {

        //job超时时间为5000 Ms，即5秒，初始生命值为2，即同一个job最多重试1次。
        return new RedisBlockingDelayQueue<ImportantDelayJob>(queueRedisTemplate,
                "MYSELF-QUEUE",
                5000L,
                2,
                ImportantDelayJob.class
                );
    }

```

8. 超简洁版，仅需提供一个StringRedisTemplate，其他皆使用默认值

``` java
    @Configuration
    public class RedisQueueConfiguration {

        @Bean("queueRedisTemplate")
        public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory){
            StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
            stringRedisTemplate.setConnectionFactory(redisConnectionFactory);
            stringRedisTemplate.setDefaultSerializer(new StringRedisSerializer());
            return stringRedisTemplate;
        }

    }
```

``` java

    @Service
    public class TestService {

        @Autowired
        private RedisBlockingDelayQueue<StringDelayedJob> redisBlockingDelayQueue;

        public void test() throws InterruptedException{
            //系统当前时间
            long offerTime = System.currentTimeMillis();
            //创建一个job，希望在当前时间的五秒过后才能被消费者消费
            StringDelayedJob putJob = StringDelayedJob.build("testMessage", offerTime + 5000);
            //把job放入队列中
            redisBlockingDelayQueue.put(putJob);
            //消费队列中的job，目的是打印job中的message
            redisBlockingDelayQueue.take((Consumer<StringDelayedJob>) stringDelayedJob -> {
                System.out.println(stringDelayedJob.getMessage());
            });
        }
    }
```

### 注意事项
* 当下版本，每次创建RedisBlockingDelayQueue实例的时候，都会创建一套线程池，目前是5个线程，并且会占用一个RedisConnection。在同一个系统中，同一个queue建议用单例模式管理，后续版本会优化这个逻辑

### Todo List
* 优化线程池，多个Queue共用部分线程，减少资源占用
* 添加注解的消费方式
* 优化retry性能
* retry 插件化
* 实现容量限制
* BRPOPLPUSH操作添加超时
* stop时无法停止listen线程