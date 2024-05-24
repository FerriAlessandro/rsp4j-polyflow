package shared.contentimpl.factories;

import graph.jena.content.EmptyContent;
import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.graph.GraphFactory;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.model.Action;
import org.statefulj.fsm.model.impl.StateImpl;
import org.statefulj.persistence.memory.MemoryPersisterImpl;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import shared.contentimpl.content.SEQContent;

import java.util.LinkedList;
import java.util.List;

public class CEPContentFactory implements ContentFactory<Graph, Graph, Graph> {


    OpBGP patternA;
    OpBGP patternB;

    org.statefulj.fsm.model.State<SEQContent> stateA = new StateImpl<>("A");
    org.statefulj.fsm.model.State<SEQContent> stateB = new StateImpl<>("B");
    org.statefulj.fsm.model.State<SEQContent> stateC = new StateImpl<>("end", true);

    List<org.statefulj.fsm.model.State<SEQContent>> states = new LinkedList<>();

    MemoryPersisterImpl<SEQContent> persister;

    FSM<SEQContent> fsm = new FSM<>("Foo FSM", persister);

    SEQContent content = new SEQContent(patternA, patternB, fsm);

    Graph emptyContent;

    public CEPContentFactory(OpBGP patternA, OpBGP patternB) {

        this.patternA = patternA;
        this.patternB = patternB;

        states.add(stateA);
        states.add(stateB);
        states.add(stateC);

        this.persister = new MemoryPersisterImpl<>(states, stateA);
        FSM<SEQContent> fsm = new FSM<>("Foo FSM", persister);
        Accumulation action = new Accumulation();
        stateA.addTransition("A", stateB, action);
        stateA.addTransition("B", stateC, action);

        this.emptyContent = GraphFactory.createGraphMem();
    }

    @Override
    public Content<Graph, Graph, Graph> createEmpty() {
        return new EmptyContent<>(emptyContent);
    }

    @Override
    public Content<Graph, Graph, Graph> create() {
        FSM<SEQContent> fsm = new FSM<>("Foo FSM", new MemoryPersisterImpl<>(states, stateA));
        Accumulation action = new Accumulation();
        stateA.addTransition("A", stateB, action);
        stateA.addTransition("B", stateC, action);
        this.emptyContent = GraphFactory.createGraphMem();
        return new SEQContent(patternA, patternB, fsm);
    }

    //This corresponds to the f1
    public static class Accumulation implements Action<SEQContent> {
        public void execute(SEQContent stateful,
                            String event,
                            Object... args) throws RetryException {
            stateful.history.add((Graph) args[0]);
        }
    }
}
