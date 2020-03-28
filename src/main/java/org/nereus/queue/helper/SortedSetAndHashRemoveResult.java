package org.nereus.queue.helper;

/**
 * @Description:
 * @author: nereus east
 * @Data: 2020/3/25 15:56
 */
public class SortedSetAndHashRemoveResult {
    private long sortedRemoveCount;
    private long hashRemoveCount;

    public SortedSetAndHashRemoveResult() {
    }

    public SortedSetAndHashRemoveResult(long sortedRemoveCount, long hashRemoveCount) {
        this.sortedRemoveCount = sortedRemoveCount;
        this.hashRemoveCount = hashRemoveCount;
    }

    public long getSortedRemoveCount() {
        return sortedRemoveCount;
    }

    public void setSortedRemoveCount(long sortedRemoveCount) {
        this.sortedRemoveCount = sortedRemoveCount;
    }

    public long getHashRemoveCount() {
        return hashRemoveCount;
    }

    public void setHashRemoveCount(long hashRemoveCount) {
        this.hashRemoveCount = hashRemoveCount;
    }
}
