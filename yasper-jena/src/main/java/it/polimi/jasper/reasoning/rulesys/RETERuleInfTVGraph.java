package it.polimi.jasper.reasoning.rulesys;

import it.polimi.jasper.reasoning.JenaTimeVaryingInfGraph;
import it.polimi.yasper.core.query.operators.s2r.WindowOperator;
import org.apache.jena.graph.Graph;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.RETERuleInfGraph;
import org.apache.jena.reasoner.rulesys.Rule;

import java.util.List;

/**
 * Created by riccardo on 05/07/2017.
 */
public class RETERuleInfTVGraph extends RETERuleInfGraph implements JenaTimeVaryingInfGraph {

    private long last_timestamp;
    private WindowOperator window;


    public RETERuleInfTVGraph() {
        super(null, null);
    }

    public RETERuleInfTVGraph(Reasoner reasoner, Graph schema, long last_timestamp, WindowOperator w) {
        super(reasoner, schema);
        this.last_timestamp = last_timestamp;
        this.window = w;
    }


    public RETERuleInfTVGraph(Reasoner reasoner, List<Rule> rules, Graph schema, long last_timestamp, WindowOperator w) {
        super(reasoner, rules, schema);
        this.last_timestamp = last_timestamp;
        this.window = w;
    }

    public RETERuleInfTVGraph(Reasoner reasoner, List<Rule> rules, Graph schema, Graph data,
                              long last_timestamp, WindowOperator w) {
        super(reasoner, rules, schema, data);
        this.last_timestamp = last_timestamp;
        this.window = w;
    }

    @Override
    public long getTimestamp() {
        return last_timestamp;
    }

    @Override
    public void setTimestamp(long ts) {
        this.last_timestamp = ts;
    }

    @Override
    public WindowOperator getWindowOperator() {
        return window;
    }

    @Override
    public void setWindowOperator(WindowOperator w) {
        this.window = w;
    }
}