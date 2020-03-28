package org.nereus.queue.helper;

import org.nereus.queue.RedisConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@SpringBootApplication(scanBasePackageClasses = RedisConfig.class)
public class RedisScriptExecuteHelperTest {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void hashCompareSetOnListMoveToSortedSet() {
        String listKey = "list-test";
        String sortedSetKey = "sorted-set-test";
        String hashKey = "hash-test";
        stringRedisTemplate.delete(listKey);
        stringRedisTemplate.delete(sortedSetKey);
        stringRedisTemplate.delete(hashKey);

        stringRedisTemplate.opsForList().leftPush(listKey, "1");
        stringRedisTemplate.opsForList().leftPush(listKey, "2");
        stringRedisTemplate.opsForList().leftPush(listKey, "3");
        stringRedisTemplate.opsForHash().put(hashKey, "1", "test 1, version1.0");
        stringRedisTemplate.opsForHash().put(hashKey, "2", "test 2, version1.0");
        //模拟并发，假设值已经被其他服务修改成2.0了
        stringRedisTemplate.opsForHash().put(hashKey, "3", "test 3, version2.0");

        List<HashCompareSetOnListMoveToSortedSetParam> hashCompareSetOnListMoveToSortedSetParams = new ArrayList<>(3);
        hashCompareSetOnListMoveToSortedSetParams.add(
                HashCompareSetOnListMoveToSortedSetParam
                        .build("1", "test 1, version1.0", "test 1, version2.0", 1));
        hashCompareSetOnListMoveToSortedSetParams.add(
                HashCompareSetOnListMoveToSortedSetParam
                        .build("2", "test 2, version1.0", "test 2, version2.0", 2));
        hashCompareSetOnListMoveToSortedSetParams.add(
                HashCompareSetOnListMoveToSortedSetParam
                        .build("3", "test 3, version1.0", "test 3, version2.0", 3));
        long updateCount = RedisScriptExecuteHelper.hashCompareSetOnListMoveToSortedSet(
                stringRedisTemplate, listKey, sortedSetKey, hashKey, hashCompareSetOnListMoveToSortedSetParams, 100);

        assertEquals(2L, updateCount);
        assertEquals("1", stringRedisTemplate.opsForZSet().range(sortedSetKey, 0, 0).toArray()[0]);
        assertEquals("2", stringRedisTemplate.opsForZSet().range(sortedSetKey, 1, 1).toArray()[0]);
        assertEquals(2, stringRedisTemplate.opsForZSet().size(sortedSetKey).intValue());

        assertEquals("test 1, version2.0", stringRedisTemplate.opsForHash().get(hashKey, "1"));
        assertEquals("test 2, version2.0", stringRedisTemplate.opsForHash().get(hashKey, "2"));
        assertEquals("test 3, version2.0", stringRedisTemplate.opsForHash().get(hashKey, "3"));

        assertEquals(1, stringRedisTemplate.opsForList().size(listKey).intValue());

        stringRedisTemplate.delete(listKey);
        stringRedisTemplate.delete(sortedSetKey);
        stringRedisTemplate.delete(hashKey);
    }

    @Test
    public void listAndHashRemove() {
        String listKey = "list-test";
        String hashKey = "hash-test";
        stringRedisTemplate.delete(listKey);
        stringRedisTemplate.delete(hashKey);

        stringRedisTemplate.opsForList().leftPush(listKey, "1");
        stringRedisTemplate.opsForList().leftPush(listKey, "2");
        stringRedisTemplate.opsForHash().put(hashKey, "1", "test");
        stringRedisTemplate.opsForHash().put(hashKey, "2", "test");
        stringRedisTemplate.opsForHash().put(hashKey, "3", "test");

        assertTrue(RedisScriptExecuteHelper.listAndHashRemove(stringRedisTemplate, listKey, hashKey, "1"));
        assertTrue(RedisScriptExecuteHelper.listAndHashRemove(stringRedisTemplate, listKey, hashKey, "2"));
        assertFalse(RedisScriptExecuteHelper.listAndHashRemove(stringRedisTemplate, listKey, hashKey, "2"));

        assertNull(stringRedisTemplate.opsForList().leftPop(listKey));
        assertEquals("test", stringRedisTemplate.opsForHash().get(hashKey, "3"));

        stringRedisTemplate.delete(listKey);
        stringRedisTemplate.delete(hashKey);
    }


    @Test
    public void listRightPopLeftPushAndBulkGet() {
        String popListKey = "list";
        String pushListKey = "back_list";
        stringRedisTemplate.delete(popListKey);
        stringRedisTemplate.delete(pushListKey);

        stringRedisTemplate.opsForList().leftPush(popListKey, "1");
        stringRedisTemplate.opsForList().leftPush(popListKey, "2");
        stringRedisTemplate.opsForList().leftPush(popListKey, "3");
        stringRedisTemplate.opsForList().leftPush(popListKey, "4");

        List<String> results = RedisScriptExecuteHelper.listRightPopLeftPushAndBulkGet(stringRedisTemplate, popListKey, pushListKey, 2);
        assertArrayEquals(new String[]{"1", "2"}, results.toArray(new String[2]));
        String firstValue = stringRedisTemplate.opsForList().rightPop(pushListKey);
        String secondValue = stringRedisTemplate.opsForList().rightPop(pushListKey);
        assertEquals("back_list push error", "1", firstValue);
        assertEquals("back_list push error", "2", secondValue);


        List<String> resultsAgain = RedisScriptExecuteHelper.listRightPopLeftPushAndBulkGet(stringRedisTemplate, popListKey, pushListKey, 100);
        assertEquals(2, resultsAgain.size());

        stringRedisTemplate.delete(popListKey);
        stringRedisTemplate.delete(pushListKey);
    }

    @Test
    public void listWithHashMGet() {
        String listKey = "list-test";
        String hashKey = "hash-test";
        stringRedisTemplate.delete(listKey);
        stringRedisTemplate.delete(hashKey);

        stringRedisTemplate.opsForList().leftPush(listKey, "1");
        stringRedisTemplate.opsForList().leftPush(listKey, "2");
        stringRedisTemplate.opsForHash().put(hashKey, "1", "test1");
        stringRedisTemplate.opsForHash().put(hashKey, "2", "test2");
        stringRedisTemplate.opsForHash().put(hashKey, "3", "test3");
        String[] results = RedisScriptExecuteHelper.listWithHashMGet(stringRedisTemplate, listKey, hashKey, 0, -1, true).toArray(new String[2]);
        assertArrayEquals(new String[]{"test1", "test2"}, results);

        stringRedisTemplate.delete(listKey);
        stringRedisTemplate.delete(hashKey);
        assertEquals(0, RedisScriptExecuteHelper.listWithHashMGet(stringRedisTemplate, listKey, hashKey, 0, -1, true).size());
    }

    @Test
    public void sortedFirstWithHashGet() {
        String zsetKey = "sorted-set-test";
        String hashKey = "hash-test";

        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(hashKey);

        String idOne = UUID.randomUUID().toString();
        stringRedisTemplate.opsForZSet().add(zsetKey, idOne, 1d);
        stringRedisTemplate.opsForHash().put(hashKey, idOne, "1");
        String idZero = UUID.randomUUID().toString();
        stringRedisTemplate.opsForZSet().add(zsetKey, idZero, 0d);
        stringRedisTemplate.opsForHash().put(hashKey, idZero, "0");
        String idTwo = UUID.randomUUID().toString();
        stringRedisTemplate.opsForZSet().add(zsetKey, idTwo, 2d);
        stringRedisTemplate.opsForHash().put(hashKey, idTwo, "2");

        String first = RedisScriptExecuteHelper.sortedFirstWithHashGet(stringRedisTemplate, zsetKey, hashKey);
        assertEquals("sortedFirstWithHashGet error", "0", first);
        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(hashKey);
    }

    @Test
    public void sortedSetAndHashAdd() {
        String zsetKey = "sorted-set-test";
        String hashKey = "hash-test";

        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(hashKey);

        String id = UUID.randomUUID().toString();
        RedisScriptExecuteHelper.sortedSetAndHashAdd(stringRedisTemplate, zsetKey, hashKey, id, "test", 0d);

        assertEquals("test", stringRedisTemplate.opsForHash().get(hashKey, id));
        Set<String> contentSet = stringRedisTemplate.opsForZSet().rangeByScore(zsetKey, 0d, 0d);
        assertNotNull(contentSet);
        assertEquals(id, contentSet.toArray()[0]);

        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(hashKey);
    }

    @Test
    public void sortedSetAndHashRemove() {
        String zsetKey = "zset-test";
        String hashKey = "hash-test";
        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(hashKey);

        stringRedisTemplate.opsForZSet().add(zsetKey, "1", 1);
        stringRedisTemplate.opsForZSet().add(zsetKey, "2", 2);
        stringRedisTemplate.opsForZSet().add(zsetKey, "3", 3);
        stringRedisTemplate.opsForHash().put(hashKey, "1", "3");
        stringRedisTemplate.opsForHash().put(hashKey, "2", "3");
        stringRedisTemplate.opsForHash().put(hashKey, "3", "3");
        stringRedisTemplate.opsForHash().put(hashKey, "4", "3");
        stringRedisTemplate.opsForHash().put(hashKey, "5", "3");
        SortedSetAndHashRemoveResult sortedSetAndHashRemoveResult =
                RedisScriptExecuteHelper
                        .sortedSetAndHashRemove(stringRedisTemplate, zsetKey, hashKey, new String[]{"1", "2", "3", "4"});

        assertEquals(3, sortedSetAndHashRemoveResult.getSortedRemoveCount());
        assertEquals(4, sortedSetAndHashRemoveResult.getHashRemoveCount());
        assertEquals("3", stringRedisTemplate.opsForHash().get(hashKey, "5"));
        assertNull(stringRedisTemplate.opsForHash().get(hashKey, "1"));
        assertNull(stringRedisTemplate.opsForHash().get(hashKey, "2"));
        assertNull(stringRedisTemplate.opsForHash().get(hashKey, "3"));
        assertNull(stringRedisTemplate.opsForHash().get(hashKey, "4"));

        assertEquals(0, stringRedisTemplate.opsForZSet().range(zsetKey, 0, 0).size());

        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(hashKey);
    }

    @Test
    public void sortedSetAndListCount() {
        String zsetKey = "zset-test";
        String listKey = "list-test";
        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(listKey);

        stringRedisTemplate.opsForList().leftPush(listKey, "1");
        stringRedisTemplate.opsForList().leftPush(listKey, "2");
        stringRedisTemplate.opsForList().leftPush(listKey, "3");
        stringRedisTemplate.opsForList().leftPush(listKey, "4");
        stringRedisTemplate.opsForZSet().add(zsetKey, "1", 1);
        stringRedisTemplate.opsForZSet().add(zsetKey, "2", 2);
        stringRedisTemplate.opsForZSet().add(zsetKey, "3", 3);

        assertEquals(7, RedisScriptExecuteHelper.sortedSetAndListCount(stringRedisTemplate, zsetKey, listKey));

        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(listKey);
    }

    @Test
    public void sortedSetToList() {
        String zsetKey = "zset-test";
        String listKey = "list-test";
        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(listKey);

        stringRedisTemplate.opsForZSet().add(zsetKey, "1", 1);
        stringRedisTemplate.opsForZSet().add(zsetKey, "2", 2);
        stringRedisTemplate.opsForZSet().add(zsetKey, "3", 3);

        long moves = RedisScriptExecuteHelper.sortedSetToList(stringRedisTemplate, zsetKey, listKey, 2, 3, 1L);
        assertEquals(1L, moves);
        String[] strings = stringRedisTemplate.opsForZSet().range(zsetKey, 0, -1).toArray(new String[2]);
        assertArrayEquals(new String[]{"1", "3"}, strings);
        assertEquals("2", stringRedisTemplate.opsForList().rightPop(listKey));
        assertEquals(null, stringRedisTemplate.opsForList().rightPop(listKey));

        assertEquals(0, RedisScriptExecuteHelper.sortedSetToList(stringRedisTemplate, zsetKey, listKey, 0, 0, 1L));

        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(listKey);
    }

    @Test
    public void sortedSetToList1() {
        String zsetKey = "zset-test";
        String listKey = "list-test";
        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(listKey);

        stringRedisTemplate.opsForZSet().add(zsetKey, "1", 1);
        stringRedisTemplate.opsForZSet().add(zsetKey, "2", 2);
        stringRedisTemplate.opsForZSet().add(zsetKey, "3", 3);

        RedisScriptExecuteHelper.sortedSetToList(stringRedisTemplate, zsetKey, listKey, 1, 3);
        assertEquals(0, stringRedisTemplate.opsForZSet().range(zsetKey, 0, -1).size());
        assertEquals("1", stringRedisTemplate.opsForList().rightPop(listKey));
        assertEquals("2", stringRedisTemplate.opsForList().rightPop(listKey));
        assertEquals("3", stringRedisTemplate.opsForList().rightPop(listKey));

        stringRedisTemplate.delete(zsetKey);
        stringRedisTemplate.delete(listKey);
    }
}