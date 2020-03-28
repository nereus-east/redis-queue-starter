package org.nereus.queue.config;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.util.List;

/**
 * @description: Redis helper configuration
 * @author: nereus east
 * @data: 2020/3/21 20:57
 */
public class RedisScriptConfiguration {

    private final static DefaultRedisScript<Long> hashCompareSetOnListMoveToSortedSetScript = new DefaultRedisScript<>();

    static {
        hashCompareSetOnListMoveToSortedSetScript.setResultType(Long.class);
        hashCompareSetOnListMoveToSortedSetScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/HashCompareSetOnListMoveToSortedSet.lua")));
    }

    private final static DefaultRedisScript<Long> listAndHashRemoveScript = new DefaultRedisScript<>();

    static {
        listAndHashRemoveScript.setResultType(Long.class);
        listAndHashRemoveScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/ListAndHashRemove.lua")));
    }

    private final static DefaultRedisScript<List> listRightPopLeftPushAndBulkGetScript = new DefaultRedisScript<>();

    static {
        listRightPopLeftPushAndBulkGetScript.setResultType(List.class);
        listRightPopLeftPushAndBulkGetScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/ListRightPopLeftPushAndBulkGet.lua")));
    }

    private final static DefaultRedisScript<List> listWithHashMGetScript = new DefaultRedisScript<>();

    static {
        listWithHashMGetScript.setResultType(List.class);
        listWithHashMGetScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/ListWithHashMGet.lua")));
    }

    private final static DefaultRedisScript<String> sortedFirstWithHashGetScript = new DefaultRedisScript<>();

    static {
        sortedFirstWithHashGetScript.setResultType(String.class);
        sortedFirstWithHashGetScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/SortedFirstWithHashGet.lua")));
    }

    private final static DefaultRedisScript<Long> sortedSetAndHashAddScript = new DefaultRedisScript<>();

    static {
        sortedSetAndHashAddScript.setResultType(Long.class);
        sortedSetAndHashAddScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/SortedSetAndHashAdd.lua")));
    }

    private final static DefaultRedisScript<List> sortedSetAndHashRemoveScript = new DefaultRedisScript<>();

    static {
        sortedSetAndHashRemoveScript.setResultType(List.class);
        sortedSetAndHashRemoveScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/SortedSetAndHashRemove.lua")));
    }

    private final static DefaultRedisScript<Long> sortedSetAndListCountScript = new DefaultRedisScript<>();

    static {
        sortedSetAndListCountScript.setResultType(Long.class);
        sortedSetAndListCountScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/SortedSetAndListCount.lua")));
    }

    private final static DefaultRedisScript<Long> sortedSetToListScript = new DefaultRedisScript<>();

    static {
        sortedSetToListScript.setResultType(Long.class);
        sortedSetToListScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/SortedSetToList.lua")));
    }

    public static RedisScript<Long> getHashCompareSetOnListMoveToSortedSetScript() {
        return hashCompareSetOnListMoveToSortedSetScript;
    }

    public static RedisScript<Long> getListAndHashRemoveScript() {
        return listAndHashRemoveScript;
    }

    public static RedisScript<List> getListRightPopLeftPushAndBulkGetScript() {
        return listRightPopLeftPushAndBulkGetScript;
    }

    public static RedisScript<List> getListWithHashMGetScript() {
        return listWithHashMGetScript;
    }

    public static RedisScript<String> getSortedFirstWithHashGetScript() {
        return sortedFirstWithHashGetScript;
    }

    public static RedisScript<Long> getSortedSetAndHashAddScript() {
        return sortedSetAndHashAddScript;
    }

    public static RedisScript<List> getSortedSetAndHashRemoveScript() {
        return sortedSetAndHashRemoveScript;
    }

    public static RedisScript<Long> getSortedSetAndListCountScript() {
        return sortedSetAndListCountScript;
    }

    public static RedisScript<Long> getSortedSetToListScript() {
        return sortedSetToListScript;
    }
}
