package org.streamreasoning.rsp4j.api.secret.content;


import org.streamreasoning.rsp4j.api.secret.report.ReportStrategies;
import org.streamreasoning.rsp4j.api.secret.report.strategies.ReportingStrategy;

public interface Content<I, W, R> {

    int size();

    void add(I e);

    R coalesce();

    default boolean visit(ReportingStrategy s){
        return false;
    }

    default boolean isClosed() {
        return false;
    }

}
