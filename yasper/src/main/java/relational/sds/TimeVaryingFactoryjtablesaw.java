package relational.sds;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVaryingFactory;

public class TimeVaryingFactoryjtablesaw<R extends Iterable<?>> implements TimeVaryingFactory<R> {
    @Override
    public TimeVarying<R> create(StreamToRelationOperator<?, ?, R> s2r, String name) {
        return new TimeVaryingObjectjtablesaw<>(s2r, name);
    }
}
