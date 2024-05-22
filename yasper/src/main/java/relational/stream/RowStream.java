package relational.stream;

import org.apache.jena.graph.Graph;
import org.javatuples.Quartet;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import tech.tablesaw.api.Row;

import java.util.ArrayList;
import java.util.List;

public class RowStream<X> implements DataStream<X> {

    String URI;
    protected List<Consumer<X>> consumers = new ArrayList<>();

    public RowStream(String streamURI){
        this.URI = streamURI;
    }
    @Override
    public void addConsumer(Consumer<X> windowAssigner) {
        if(!consumers.contains(windowAssigner))
            this.consumers.add(windowAssigner);
    }

    @Override
    public void put(X row, long ts) {
        consumers.forEach(graphConsumer -> graphConsumer.notify(this, row, ts));
    }

    @Override
    public String getName() {
        return URI;
    }
}
