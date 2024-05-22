import graph.jena.datatypes.JenaGraphOrBindings;
import graph.jena.operatorsimpl.r2r.jena.FullQueryUnaryJena;
import graph.jena.operatorsimpl.r2s.RelationToStreamOpImpl;
import graph.jena.sds.SDSJena;
import graph.jena.sds.TimeVaryingFactoryJena;
import graph.jena.stream.JenaBindingStream;
import graph.jena.stream.JenaStreamGenerator;
import org.apache.jena.graph.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.jupiter.api.Test;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.Task;
import org.streamreasoning.rsp4j.api.querying.TaskImpl;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVaryingFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import relational.stream.RowStream;
import shared.contentimpl.factories.AccumulatorContentFactory;
import shared.operatorsimpl.r2r.DAG.DAGImpl;
import shared.operatorsimpl.s2r.CSPARQLStreamToRelationOpImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskTest {


    @Test
    public void testTaskUnary() {

        /*------INITIALIZATION OF COMPONENTS USED BY THE TASK------*/

        JenaStreamGenerator generator = new JenaStreamGenerator();
        DataStream<Graph> inputStreamColors = generator.getStream("http://test/stream1");
        JenaBindingStream outStream = new JenaBindingStream("out");
        Report report = new ReportImpl();
        report.add(new OnWindowClose());
        Tick tick = Tick.TIME_DRIVEN;
        ReportGrain report_grain = ReportGrain.SINGLE;
        Time instance = new TimeImpl(0);
        JenaGraphOrBindings emptyContent = new JenaGraphOrBindings(GraphFactory.createGraphMem());
        TimeVaryingFactory<JenaGraphOrBindings> tvFactory = new TimeVaryingFactoryJena();

        AccumulatorContentFactory<Graph, Graph, JenaGraphOrBindings> accumulatorContentFactory = new AccumulatorContentFactory<>((g) -> g, (g) -> new JenaGraphOrBindings(g), (r1, r2) -> {
            Model m1 = ModelFactory.createModelForGraph(r1.getContent());
            Model m2 = ModelFactory.createModelForGraph(r2.getContent());
            Graph res_content = m1.union(m2).getGraph();
            return new JenaGraphOrBindings(res_content);
        }, emptyContent);
        StreamToRelationOperator<Graph, Graph, JenaGraphOrBindings> s2rOp_one = new CSPARQLStreamToRelationOpImpl<>(tick, instance, "w1", accumulatorContentFactory, tvFactory, report_grain, report, 1000, 1000);


        List<String> s2r_names = new ArrayList<>();
        s2r_names.add(s2rOp_one.getName());
        RelationToRelationOperator<JenaGraphOrBindings> r2rOp = new FullQueryUnaryJena("SELECT * WHERE {GRAPH ?g {?s ?p ?o }}", s2r_names, "selection");
        RelationToStreamOperator<JenaGraphOrBindings, Binding> r2sOp = new RelationToStreamOpImpl();
        Task<Graph, Graph, JenaGraphOrBindings, Binding> task = new TaskImpl<>();

        task.addS2ROperator(s2rOp_one, inputStreamColors).addR2ROperator(r2rOp).addR2SOperator(r2sOp).addDAG(new DAGImpl<>()).addSDS(new SDSJena()).addTime(instance);
        task.initialize();
        /*-------------END OF INITIALIZATION----------------*/


        /*---------------Test the addS2ROperator method-------------*/

        //Create a dummy S2R to check if the Task correctly throws an exception when an S2R with the same name is already present
        StreamToRelationOperator<Graph, Graph, JenaGraphOrBindings> s2rOp_dummy = new CSPARQLStreamToRelationOpImpl<>(tick, instance, "w1", accumulatorContentFactory, tvFactory, report_grain, report, 500, 500);


        noDuplicateTests(s2rOp_dummy, task);

        /*---------------End of test addS2ROperator method-------------*/

        /*---------------Beginning test elaborateElement method-------------*/
        Graph graph1 = GraphMemFactory.createGraphMem();
        Node p = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        graph1.add(NodeFactory.createURI("http://test/S0"), p, NodeFactory.createURI("http://test/Red"));
        graph1.add(NodeFactory.createURI("http://test/S1"), p, NodeFactory.createURI("http://test/Black"));
        elaborateElementTest(inputStreamColors, graph1, r2rOp, task);

    }


    public void noDuplicateTests(StreamToRelationOperator<Graph, Graph, JenaGraphOrBindings> s2rOp_dummy, Task<Graph, Graph, JenaGraphOrBindings, Binding> task) {

        assertThrows(RuntimeException.class, () -> task.addS2ROperator(s2rOp_dummy, new RowStream<>("foo")));
    }


    public void elaborateElementTest(DataStream<Graph> inputStream, Graph g, RelationToRelationOperator<JenaGraphOrBindings> r2r, Task<Graph, Graph, JenaGraphOrBindings, Binding> task) {

        JenaGraphOrBindings dataset1 = new JenaGraphOrBindings(g);
        JenaGraphOrBindings dataset2 = new JenaGraphOrBindings(g);
        //dataset will now hold a list of bindings, result of the computation of the R2R
        r2r.eval(List.of(dataset1,dataset2));

        //Insert element in window 0-1000
        task.elaborateElement(inputStream, g, 500);
        //Make window 0-1000 close and report result by adding an element at time 1200
        Collection<Collection<Binding>> result = task.elaborateElement(inputStream, Factory.createDefaultGraph(), 1200);
        result.forEach(coll -> assertTrue(dataset1.getResult().containsAll(coll)));

    }
}
