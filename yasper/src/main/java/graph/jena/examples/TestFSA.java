package graph.jena.examples;

import org.apache.jena.graph.Graph;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.TooBusyException;
import org.statefulj.fsm.model.Action;
import org.statefulj.fsm.model.StateActionPair;
import org.statefulj.fsm.model.Transition;
import org.statefulj.fsm.model.impl.StateActionPairImpl;
import org.statefulj.fsm.model.impl.StateImpl;
import org.statefulj.persistence.annotations.State;
import org.statefulj.persistence.memory.MemoryPersisterImpl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TestFSA {

    public static void main(String[] args) throws TooBusyException {
        // Instantiate the Stateful Entity
//

        // Events
//
        String eventA = "Event A";
        String eventB = "Event B";

// States
//
        org.statefulj.fsm.model.State<Foo> stateA = new StateImpl<Foo>("State A");
        org.statefulj.fsm.model.State<Foo> stateB = new StateImpl<Foo>("State B");
        org.statefulj.fsm.model.State<Foo> stateC = new StateImpl<Foo>("State C", true); // End State


// Actions
//
        Action<Foo> actionA = new HelloAction("World");
        Action<Foo> actionB = new HelloAction("Folks");



        /* Deterministic Transitions */

// stateA(eventA) -> stateB/actionA
//
        stateA.addTransition(eventA, stateB, actionA);

// stateB(eventB) -> stateC/actionB
//
        stateB.addTransition(eventB, stateC, actionB);




        /* Non-Deterministic Transitions */

//                   +--> stateB/NOOP  -- loop back on itself
//  stateB(eventA) --|
//                   +--> stateC/NOOP
//
//        stateB.addTransition(eventA, new Transition<Foo>() {
//
//            @Override
//            public StateActionPair<Foo> getStateActionPair(Foo foo, String s, Object... objects) throws RetryException {
//                return null;
//            }
//
//            public StateActionPair<Foo> getStateActionPair(Foo stateful) {
//                org.statefulj.fsm.model.State<Foo> next = null;
//                if (stateful.isBar()) {
//                    next = stateB;
//                } else {
//                    next = stateC;
//                }
//
//                // Move to the next state without taking any action
//                //
//                return new StateActionPairImpl<Foo>(next, null);
//            }
//        });


        List<org.statefulj.fsm.model.State<Foo>> states = new LinkedList<org.statefulj.fsm.model.State<Foo>>();
        states.add(stateA);
        states.add(stateB);
        states.add(stateC);

        MemoryPersisterImpl<Foo> persister =
                new MemoryPersisterImpl<Foo>(
                        states,   // Set of States
                        stateA);  // Start State


        FSM<Foo> fsm = new FSM<Foo>("Foo FSM", persister);


        Foo foo = new Foo();

// Drive the FSM with a series of events: eventA, eventA, eventA
//
        fsm.onEvent(foo, eventA);  // stateA(EventA) -> stateB/actionA

        foo.setBar(true);

        fsm.onEvent(foo, eventA);  // stateB(EventA) -> stateB/NOOP

        foo.setBar(false);

        fsm.onEvent(foo, eventB);  // stateB(EventA) -> stateC/NOOP

        System.out.println(foo.state);

        foo.history.forEach(System.out::println);

    }

    // Hello <what> Action
//
    public static class HelloAction implements Action<Foo> {

        String what;

        public HelloAction(String what) {
            this.what = what;
        }

        public void execute(Foo stateful,
                            String event,
                            Object... args) throws RetryException {
            System.out.println("Hello " + what);
            stateful.history.add(event);

        }
    }

    public static class Foo {

        @State
        Graph state;   // Memory Persister requires a String

        List<String> history = new ArrayList<>();

        boolean bar;

        public Graph getState() {
            return state;
        }

        // Note: there is no setter for the state field
        //       as the value is set by StatefulJ

        public void setBar(boolean bar) {
            this.bar = bar;
        }

        public boolean isBar() {
            return bar;
        }




    }

}
