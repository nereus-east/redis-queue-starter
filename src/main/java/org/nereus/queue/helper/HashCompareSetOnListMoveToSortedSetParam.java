package org.nereus.queue.helper;

import org.springframework.util.StringUtils;

import java.security.InvalidParameterException;

/**
 * @Description: Encapsulate parameters for easy invocation
 * @author: nereus east
 * @Data: 2020/3/26 10:07
 */
public class HashCompareSetOnListMoveToSortedSetParam {

    private String id;
    private String oldContent;
    private String content;
    private double score;

    public static HashCompareSetOnListMoveToSortedSetParam build(String id, String oldContent, String content, double score) {
        return new HashCompareSetOnListMoveToSortedSetParam(id, oldContent, content, score);
    }

    public HashCompareSetOnListMoveToSortedSetParam(String id, String oldContent, String content, double score) {
        if (StringUtils.isEmpty(id)) {
            throw new InvalidParameterException("Call hashCompareSetOnListMoveToSortedSet ，id must not null");
        }
        if (StringUtils.isEmpty(oldContent)) {
            throw new InvalidParameterException("Call hashCompareSetOnListMoveToSortedSet ，oldContent must not null");
        }
        if (StringUtils.isEmpty(content)) {
            throw new InvalidParameterException("Call hashCompareSetOnListMoveToSortedSet ，content must not null");
        }
        this.id = id;
        this.oldContent = oldContent;
        this.content = content;
        this.score = score;
    }

    public String getId() {
        return id;
    }

    public String getOldContent() {
        return oldContent;
    }

    public String getContent() {
        return content;
    }

    public double getScore() {
        return score;
    }
}
