package org.nereus.queue.helper;

public enum SortedSetAndHashPutResult {
    ADD_SUCCESS,
    UPDATE_SUCCESS,
    HASH_EXIST_ZSET_NOT_EXIST,
    HASH_NOT_EXIST_ZSET_EXIST;

    public final int mask;

    public int getMask() {
        return mask;
    }

    SortedSetAndHashPutResult() {
        mask = (1 << ordinal());
    }

    public static boolean success(SortedSetAndHashPutResult result) {
        return ((ADD_SUCCESS.mask | UPDATE_SUCCESS.mask) & result.mask) != 0;
    }
}
