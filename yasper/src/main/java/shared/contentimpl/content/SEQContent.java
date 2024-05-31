package shared.contentimpl.content;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.log4j.Logger;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.TooBusyException;
import org.statefulj.persistence.annotations.State;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import shared.contentimpl.factories.SEQContentFactory;

import java.util.ArrayList;
import java.util.List;

public class SEQContent implements Content<Event<Graph>, Graph, JenaGraphOrBindings> {

    private static final Logger log = Logger.getLogger(SEQContent.class);
    private final SEQContentFactory factory;

    @State
    public String state;   // Memory Persister requires a String

    public List<Event<Graph>> history = new ArrayList<>();
    public List<List<Binding>> results = new ArrayList<>();


    final OpBGP patternA;
    final OpBGP patternB;

    FSM<SEQContent> fsm;

    public SEQContent(OpBGP patternA, OpBGP patternB, FSM<SEQContent> fsm, SEQContentFactory seqContentFactory, Window w) {
        this.patternA = patternA;
        this.patternB = patternB;
        this.fsm = fsm;
        this.state = "A";
        this.factory = seqContentFactory;
    }

    public String getState() {
        return state;
    }

    @Override
    public int size() {
        return history.size();
    }

    @Override
    public void add(Event<Graph> e) {
        try {
            QueryIterator iter;
            if ("A".equals(state)) {
                iter = Algebra.exec(patternA, e.get());
                while (iter.hasNext()) {
                    log.debug(fsm.getName() + " found and A");
                    fsm.onEvent(this, "A", e);
                    log.debug(fsm.getName() + " is now looking for a [" + this.state + "]");
                    iter.close();
                }
            } else if ("B".equals(state)) {
                iter = Algebra.exec(patternB, e.get());
                while (iter.hasNext()) {
                    log.debug(fsm.getName() + " found and B ");
                    fsm.onEvent(this, "B", e, fsm);
                    log.debug(fsm.getName() + " is now  done [" + this.state + "]");
//                    String name = fsm.getName();
//                    log.debug("Creating a new FSM ");
//                    fsm = factory.getSeqContentFSM();
//                    fsm.setName(name);
                    iter.close();
                }
            }
        } catch (TooBusyException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public JenaGraphOrBindings coalesce() {
        //This corresponds to the f2
        Graph reduce = this.history.stream().map(Event::get).reduce(GraphFactory.createGraphMem(), (l, r) -> {
            r.stream().forEach(l::add);
            return l;
        });


        return new JenaGraphOrBindings(reduce);
    }

    @Override
    public boolean isClosed() {
        return "MATCHED".equals(state);
    }


}
