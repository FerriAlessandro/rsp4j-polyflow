package shared.contentimpl.factories;

import graph.jena.content.EmptyContent;
import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.model.Action;
import org.statefulj.fsm.model.State;
import org.statefulj.fsm.model.impl.StateImpl;
import org.statefulj.persistence.memory.MemoryPersisterImpl;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import shared.contentimpl.content.Event;
import shared.contentimpl.content.SEQContent;

import java.util.LinkedList;
import java.util.List;

public class SEQContentFactory implements ContentFactory<Event<Graph>, Graph, JenaGraphOrBindings> {

    final OpBGP patternA;
    final OpBGP patternB;
    private Integer counter = 0;

    Graph emptyContent;

    List<State<SEQContent>> states = new LinkedList<>();

    StateImpl<SEQContent> a = new StateImpl<>("A");
    StateImpl<SEQContent> b = new StateImpl<>("B");
    StateImpl<SEQContent> matched = new StateImpl<>("MATCHED", true);
    Accumulation add = new Accumulation();
    Accumulation star = new Accumulation();


    public SEQContentFactory(OpBGP patternA, OpBGP patternB) {

        this.patternA = patternA;
        this.patternB = patternB;

        states.add(a);
        states.add(b);
        states.add(matched);

        this.emptyContent = GraphFactory.createGraphMem();
    }

    @Override
    public Content<Event<Graph>, Graph, JenaGraphOrBindings> createEmpty() {
        return new EmptyContent<>(new JenaGraphOrBindings());
    }

    @Override
    public Content<Event<Graph>, Graph, JenaGraphOrBindings> create(Window w) {
        return new SEQContent(patternA, patternB, getSeqContentFSM(), this, w);
    }

    public FSM<SEQContent> getSeqContentFSM() {
        String name = "FSM " + counter++;

        a.addTransition("A", b, add);
//        b.addTransition("A", b, star);

        b.addTransition("B", matched, new Action<SEQContent>() {
            @Override
            public void execute(SEQContent stateful, String event, Object... args) throws RetryException {
                add.execute(stateful, event, args);
                stateful.state = "A";
                args[1] = new FSM<>(name, new MemoryPersisterImpl<>(states, states.get(0)));
            }
        });

        FSM<SEQContent> fsm = new FSM<>(name, new MemoryPersisterImpl<>(states, states.get(0)));

        return fsm;
    }


    //This corresponds to the f1
    public static class Accumulation implements Action<SEQContent> {
        public void execute(SEQContent stateful,
                            String event,
                            Object... args) throws RetryException {
            stateful.history.add((Event<Graph>) args[0]);
        }
    }
}
