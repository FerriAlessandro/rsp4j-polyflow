package org.streamreasoning.rsp4j.api.sds.timevarying;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;

/**
 * Factory to use in order to instantiate different TimeVarying implementations at runtime.
 * The Factory implementation must override the create method, which have to return an object that implements the TimeVarying interface.
 */
public interface TimeVaryingFactory<R extends Iterable<?>> {


    TimeVarying<R> create(StreamToRelationOperator<?, ?, R> s2r, String name);

}
