package shared.contentimpl.content;

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.graph.GraphFactory;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.TooBusyException;
import org.statefulj.fsm.model.Action;
import org.statefulj.fsm.model.impl.StateImpl;
import org.statefulj.persistence.annotations.State;
import org.statefulj.persistence.memory.MemoryPersisterImpl;
import org.streamreasoning.rsp4j.api.secret.content.Content;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class CEPContent implements Content<Graph, Graph, Graph> {


    OpBGP patternA;
    OpBGP patternB;

    org.statefulj.fsm.model.State<StatefulContent> stateA = new StateImpl<>("A");
    org.statefulj.fsm.model.State<StatefulContent> stateB = new StateImpl<>("B");
    org.statefulj.fsm.model.State<StatefulContent> stateC = new StateImpl<>("end", true);

    List<org.statefulj.fsm.model.State<StatefulContent>> states = new LinkedList<>();

    MemoryPersisterImpl<StatefulContent> persister;

    FSM<StatefulContent> fsm = new FSM<>("Foo FSM", persister);

    StatefulContent content = new StatefulContent();

    Graph emptyContent;

    public CEPContent(OpBGP patternA, OpBGP patternB) {

        this.patternA = patternA;
        this.patternB = patternB;

        states.add(stateA);
        states.add(stateB);
        states.add(stateC);

        this.persister = new MemoryPersisterImpl<>(states, stateA);

        Accumulation action = new Accumulation();
        stateA.addTransition("A", stateB, action);
        stateA.addTransition("B", stateC, action);

        this.emptyContent = GraphFactory.createGraphMem();
    }


    @Override
    public int size() {
        return content != null ? 1 : 0;
    }

    @Override
    public void add(Graph e) {
        try {
            while (Algebra.exec(patternA, e).hasNext()) {
                fsm.onEvent(content, "A", e);
            }
            while (Algebra.exec(patternB, e).hasNext()) {
                fsm.onEvent(content, "B", e);
            }
        } catch (TooBusyException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Graph coalesce() {    //This corresponds to the f2

        return content == null ? emptyContent : content.history.stream().reduce(GraphFactory.createGraphMem(), (l, r) -> {
            r.stream().forEach(l::add);
            return l;
        });
    }

    private class StatefulContent {

        @State
        public String state;   // Memory Persister requires a String

        List<Graph> history = new ArrayList<>();

        public String getState() {
            return state;
        }
    }

    //This corresponds to the f1
    public static class Accumulation implements Action<StatefulContent> {
        public void execute(StatefulContent stateful,
                            String event,
                            Object... args) throws RetryException {
            stateful.history.add((Graph) args[0]);
        }
    }
}
