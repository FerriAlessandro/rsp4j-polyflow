package graph.jena.examples;

import graph.jena.datatypes.JenaGraphOrBindings;
import graph.jena.operatorsimpl.r2r.rsp.Join;
import graph.jena.operatorsimpl.r2r.rsp.Project;
import graph.jena.operatorsimpl.r2r.rsp.TP;
import graph.jena.operatorsimpl.r2s.RelationToStreamOpImpl;
import graph.jena.sds.SDSJena;
import graph.jena.sds.TimeVaryingFactoryJena;
import graph.jena.stream.JenaBindingStream;
import graph.jena.stream.JenaStreamGenerator;
import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;
import org.streamreasoning.rsp4j.api.coordinators.ContinuousProgram;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.Task;
import org.streamreasoning.rsp4j.api.querying.TaskImpl;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVaryingFactory;
import org.streamreasoning.rsp4j.api.secret.report.DisjunctiveReport;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnContentChange;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnMatch;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import shared.contentimpl.content.Event;
import shared.contentimpl.factories.SEQContentFactory;
import shared.operatorsimpl.r2r.DAG.DAGImpl;
import shared.operatorsimpl.s2r.TumblingWindowOp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

public class polyflowExampleCEP {

    public static void main(String[] args) throws InterruptedException {

        JenaStreamGenerator generator = new JenaStreamGenerator();

        DataStream<Graph> inputStream = generator.getStream("http://test/stream1");

        DataStream<Event<Graph>> eventDataStream = new DataStreamImpl<>(inputStream.getName());

        inputStream.map(eventDataStream, (ts, graph) -> new Event<Graph>() {
            @Override
            public long timestamp() {
                return ts;
            }

            @Override
            public Graph get() {
                return graph;
            }
        });

        // define output stream
        JenaBindingStream outStream = new JenaBindingStream("out");

        // Engine properties
        Report report = new DisjunctiveReport();
        //report.add(new OnEviction());
        //report.add(new OnMatch());
        //report.add(new OnContentChange());
        report.add(new OnWindowClose());

        Tick tick = Tick.TIME_DRIVEN;
        ReportGrain report_grain = ReportGrain.SINGLE;
        Time instance = new TimeImpl(0);

        JenaGraphOrBindings emptyContent = new JenaGraphOrBindings(GraphFactory.createGraphMem());

        SEQContentFactory accumulatorContentFactory = new SEQContentFactory((OpBGP) Algebra.parse("(bgp (?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/Green>))"), (OpBGP) Algebra.parse("(bgp (?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://test/Red>))"));

        TimeVaryingFactory<JenaGraphOrBindings> tvFactory = new TimeVaryingFactoryJena();

        ContinuousProgram<Event<Graph>, Graph, JenaGraphOrBindings, Binding> cp = new ContinuousProgram<>();

        StreamToRelationOperator<Event<Graph>, Graph, JenaGraphOrBindings> s2rOp =
                new TumblingWindowOp<>(
                        tick,
                        instance,
                        "w1",
                        accumulatorContentFactory,
                        tvFactory,
                        report_grain,
                        report,
                        5000);


        RelationToRelationOperator<JenaGraphOrBindings> r2rOp1 = new TP((OpBGP) Algebra.parse("(bgp (?s ?p ?o))"), Collections.singletonList(s2rOp.getName()), "bgp1");
        RelationToRelationOperator<JenaGraphOrBindings> r2rOp2 = new TP((OpBGP) Algebra.parse("(bgp (?s ?p ?t))"), Collections.singletonList(s2rOp.getName()), "bgp2");

        RelationToRelationOperator<JenaGraphOrBindings> r2rOp3 = new Join(List.of("bgp1", "bgp2"), true, "empty", "join");

        RelationToRelationOperator<JenaGraphOrBindings> r2rOp4 = new Project(new OpProject(null, Collections.singletonList(Var.alloc("s"))), Collections.singletonList(s2rOp.getName()), false, "select", "empty");

        RelationToStreamOperator<JenaGraphOrBindings, Binding> r2sOp = new RelationToStreamOpImpl();

        Task<Event<Graph>, Graph, JenaGraphOrBindings, Binding> task = new TaskImpl<>();
        task = task.addS2ROperator(s2rOp, eventDataStream)
                .addR2ROperator(r2rOp1)
//                .addR2ROperator(r2rOp2)
//                .addR2ROperator(r2rOp3)
//                .addR2ROperator(r2rOp4)
                .addR2SOperator(r2sOp)
                .addDAG(new DAGImpl<>())
                .addSDS(new SDSJena())
                .addTime(instance);
        task.initialize();

        List<DataStream<Event<Graph>>> inputStreams = new ArrayList<>();
        inputStreams.add(eventDataStream);


        List<DataStream<Binding>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);

        cp.buildTask(task, inputStreams, outputStreams);

        outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();
    }
}
