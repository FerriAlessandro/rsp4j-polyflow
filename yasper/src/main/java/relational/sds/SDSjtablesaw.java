package relational.sds;


import org.apache.commons.rdf.api.IRI;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import relational.datatypes.TableWrapper;
import tech.tablesaw.api.Table;

import java.util.*;
import java.util.stream.Stream;

public class SDSjtablesaw implements SDS<Table> {

    private final List<TimeVarying<Table>> tvgs = new ArrayList<>();
    private boolean materialized = false;

    @Override
    public Collection<TimeVarying<Table>> asTimeVaryingEs() {
        return tvgs;
    }

    @Override
    public void add(String iri, TimeVarying<Table> tvg) {

    }

    @Override
    public void add(TimeVarying<Table> tvg) {
        tvgs.add(tvg);
    }

    @Override
    public SDS<Table> materialize(long ts) {

        for(TimeVarying<Table> tvg : tvgs){
            tvg.materialize(ts);
        }
        materialized();
        return this;
    }

    @Override
    public void materialized() {
        this.materialized = true;
    }

    @Override
    public Stream<Table> toStream() {
        return null;
    }
}
