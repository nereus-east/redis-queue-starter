package org.nereus.queue.helper;

import com.alibaba.fastjson.JSON;
import org.nereus.queue.config.RedisScriptConfiguration;
import org.nereus.queue.exception.RedisHashOpsException;
import org.nereus.queue.exception.RedisZSetOpsException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @description:
 * @author: nereus east
 * @data: 2020/3/21 20:58
 */
public class RedisScriptExecuteHelper {

    public static long hashCompareSetOnListMoveToSortedSet(RedisTemplate<String, String> redisTemplate,
                                                           String listKey, String sortedSetKey, String hashKey,
                                                           List<HashCompareSetOnListMoveToSortedSetParam> paramList,
                                                           long listRemoveCount) {
        Assert.notEmpty(paramList, "paramList must not empty");
        List<String> keys = new ArrayList<>(3);
        keys.add(listKey);
        keys.add(sortedSetKey);
        keys.add(hashKey);
        /**
         * The parameter is converted from [{param},{param}...] to lua table
         * like this:
         * {paramList[0].id, paramList[1].id, paramList[2].id, ...
         *  paramList[0].score, paramList[1].score, paramList[2].score, ...
         *  paramList[0].content, paramList[1].content, paramList[2].content ...}
         */
        int elementSize = paramList.size();
        String[] ids = new String[paramList.size()];
        double[] scores = new double[paramList.size()];
        String[] oldContents = new String[paramList.size()];
        String[] contents = new String[paramList.size()];
        for (int i = 0; i < elementSize; i++) {
            ids[i] = paramList.get(i).getId();
            scores[i] = paramList.get(i).getScore();
            oldContents[i] = paramList.get(i).getOldContent();
            contents[i] = paramList.get(i).getContent();
        }
        return redisTemplate.execute(
                RedisScriptConfiguration.getHashCompareSetOnListMoveToSortedSetScript(), keys,
                JSON.toJSONString(ids), JSON.toJSONString(scores),
                JSON.toJSONString(oldContents), JSON.toJSONString(contents),
                String.valueOf(listRemoveCount));
    }

    public static boolean listAndHashRemove(RedisTemplate<String, String> redisTemplate,
                                            String listKey, String hashKey, String id) {
        List<String> keys = new ArrayList<>(2);
        keys.add(listKey);
        keys.add(hashKey);
        Long result = redisTemplate.execute(RedisScriptConfiguration.getListAndHashRemoveScript(), keys, id);
        return result > 0;
    }

    public static long listAndHashRemoveBulk(RedisTemplate<String, String> redisTemplate,
                                             String listKey, String hashKey, List<String> ids,
                                             long listRemoveCount) {
        List<String> keys = new ArrayList<>(2);
        keys.add(listKey);
        keys.add(hashKey);
        Long result = redisTemplate.execute(RedisScriptConfiguration.getListAndHashRemoveBulkScript(), keys, JSON.toJSONString(ids),
                String.valueOf(listRemoveCount));
        return result.longValue();
    }

    public static List<String> listRightPopLeftPushAndBulkGet(RedisTemplate<String, String> redisTemplate,
                                                              String popListKey, String pushListKey, long limitCount) {
        List<String> keys = new ArrayList<>(2);
        keys.add(popListKey);
        keys.add(pushListKey);
        List<String> result = redisTemplate.execute(RedisScriptConfiguration.getListRightPopLeftPushAndBulkGetScript(), keys, String.valueOf(limitCount));
        return result;
    }

    public static List<String> listWithHashMGet(RedisTemplate<String, String> redisTemplate,
                                                String listKey, String hashKey, long listStart, long listEnd) {
        return listWithHashMGet(redisTemplate, listKey, hashKey, listStart, listEnd, false);
    }

    public static List<String> listWithHashMGet(RedisTemplate<String, String> redisTemplate,
                                                String listKey, String hashKey, long listStart, long listEnd, boolean reverse) {
        List<String> keys = new ArrayList<>(2);
        keys.add(listKey);
        keys.add(hashKey);
        List<String> result = redisTemplate.execute(RedisScriptConfiguration.getListWithHashMGetScript(), keys, String.valueOf(listStart), String.valueOf(listEnd));
        if (reverse) {
            Collections.reverse(result);
        }
        return result;
    }

    public static String sortedFirstWithHashGet(RedisTemplate<String, String> redisTemplate,
                                                String sortedSetKey, String hashKey) {
        List<String> keys = new ArrayList<>(2);
        keys.add(sortedSetKey);
        keys.add(hashKey);
        String result = redisTemplate.execute(RedisScriptConfiguration.getSortedFirstWithHashGetScript(), keys);
        return result;
    }

    public static boolean sortedSetAndHashAddIfAbsent(RedisTemplate<String, String> redisTemplate,
                                                      String sortedSetKey, String hashKey, String id, String content, Double score)
            throws RedisHashOpsException, RedisZSetOpsException {
        Assert.notNull(score, "score must not null");
        List<String> keys = new ArrayList<>(2);
        keys.add(sortedSetKey);
        keys.add(hashKey);
        Long result = redisTemplate.execute(RedisScriptConfiguration.getSortedSetAndHashAddIfAbsentScript(), keys, id, content, String.valueOf(score));
        return result.longValue() == 0L;
    }

    public static SortedSetAndHashPutResult sortedSetAndHashPut(RedisTemplate<String, String> redisTemplate,
                                                                String sortedSetKey, String hashKey, String id,
                                                                String content, Double score, boolean rollbackOnFail) {
        Assert.notNull(score, "score must not null");
        List<String> keys = new ArrayList<>(2);
        keys.add(sortedSetKey);
        keys.add(hashKey);
        Long result = redisTemplate.execute(RedisScriptConfiguration.getSortedSetAndHashPutScript(), keys, id, content, String.valueOf(score),
                rollbackOnFail ? String.valueOf(1) : String.valueOf(0));
        switch (result.intValue()) {
            case 0:
                return SortedSetAndHashPutResult.ADD_SUCCESS;
            case 1:
                return SortedSetAndHashPutResult.UPDATE_SUCCESS;
            case 2:
                return SortedSetAndHashPutResult.HASH_NOT_EXIST_ZSET_EXIST;
            case 3:
                return SortedSetAndHashPutResult.HASH_EXIST_ZSET_NOT_EXIST;
            default:
                throw new RuntimeException("Script [SortedSetAndHashPut] No matching results!");
        }
    }

    public static SortedSetAndHashRemoveResult sortedSetAndHashRemove(RedisTemplate<String, String> redisTemplate,
                                                                      String sortedSetKey, String hashKey, String id) {
        return sortedSetAndHashRemove(redisTemplate, sortedSetKey, hashKey, new String[]{id});
    }

    public static SortedSetAndHashRemoveResult sortedSetAndHashRemove(RedisTemplate<String, String> redisTemplate,
                                                                      String sortedSetKey, String hashKey, String[] ids) {
        List<String> keys = new ArrayList<>(2);
        keys.add(sortedSetKey);
        keys.add(hashKey);
        List<Long> result = redisTemplate.execute(RedisScriptConfiguration.getSortedSetAndHashRemoveScript(), keys, ids);
        return new SortedSetAndHashRemoveResult(result.get(0), result.get(1));
    }

    public static long sortedSetAndListCount(RedisTemplate<String, String> redisTemplate,
                                             String sortedSetKey, String listKey) {
        List<String> keys = new ArrayList<>(2);
        keys.add(sortedSetKey);
        keys.add(listKey);
        Long result = redisTemplate.execute(RedisScriptConfiguration.getSortedSetAndListCountScript(), keys);
        return result.longValue();
    }

    public static long sortedSetToList(RedisTemplate<String, String> redisTemplate,
                                       String sortedSetKey, String listKey, double minScore, double maxScore) {
        return sortedSetToList(redisTemplate, sortedSetKey, listKey, minScore, maxScore, null);
    }

    public static long sortedSetToList(RedisTemplate<String, String> redisTemplate,
                                       String sortedSetKey, String listKey, double minScore, double maxScore, Long limitCount) {
        List<String> keys = new ArrayList<>(2);
        keys.add(sortedSetKey);
        keys.add(listKey);
        Long result;
        RedisScript<Long> sortedSetToListScript = RedisScriptConfiguration.getSortedSetToListScript();

        if (limitCount == null) {
            result = redisTemplate.execute(sortedSetToListScript, keys,
                    String.valueOf(minScore), String.valueOf(maxScore));
        } else {
            result = redisTemplate.execute(sortedSetToListScript, keys,
                    String.valueOf(minScore), String.valueOf(maxScore), String.valueOf(limitCount));
        }
        return result == null ? 0 : result.longValue();
    }


}
