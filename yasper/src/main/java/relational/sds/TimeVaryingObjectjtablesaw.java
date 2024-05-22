package relational.sds;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

public class TimeVaryingObjectjtablesaw<R extends Iterable<?>> implements TimeVarying<R> {

    private final StreamToRelationOperator<?, ?, R> op;
    private String name;
    private R content;

    public TimeVaryingObjectjtablesaw(StreamToRelationOperator<?, ?, R> op, String name) {
        this.op = op;
        this.name = name;
    }

    /**
     * The setTimestamp function merges the element
     * in the content into a single graph
     * and adds it to the current dataset.
     **/
    @Override
    public void materialize(long ts) {
        content = op.content(ts).coalesce();
    }

    @Override
    public R get() {
        return content;
    }

    @Override
    public String iri() {
        return name;
    }

    @Override
    public void setIri(String name) {
        this.name = name;
    }

}
