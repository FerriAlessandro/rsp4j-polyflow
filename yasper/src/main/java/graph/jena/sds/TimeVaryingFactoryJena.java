package graph.jena.sds;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVaryingFactory;

public class TimeVaryingFactoryJena implements TimeVaryingFactory<JenaGraphOrBindings> {
    @Override
    public TimeVarying<JenaGraphOrBindings> create(StreamToRelationOperator<?, ?, JenaGraphOrBindings> s2r, String name) {
        return new TimeVaryingObject<>(s2r, RDFUtils.createIRI(name));
    }
}
