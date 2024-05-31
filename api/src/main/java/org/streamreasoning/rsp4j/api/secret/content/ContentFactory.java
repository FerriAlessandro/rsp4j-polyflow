package org.streamreasoning.rsp4j.api.secret.content;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.Window;

public interface ContentFactory<T1, T2, T3> {

    Content<T1, T2, T3> createEmpty();

    Content<T1, T2, T3> create(Window w);


}
