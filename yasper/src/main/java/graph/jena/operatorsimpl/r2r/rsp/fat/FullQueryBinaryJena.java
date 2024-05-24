package graph.jena.operatorsimpl.r2r.jena;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.compose.Union;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetImpl;
import org.apache.jena.sparql.core.ResultBinding;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.ArrayList;
import java.util.List;

public class FullQueryBinaryJena implements RelationToRelationOperator<JenaGraphOrBindings> {


    private String query;

    private List<String> tvgNames;

    private String resName;


    public FullQueryBinaryJena(String query, List<String> tvgNames, String resName) {
        this.query = query;
        this.tvgNames = tvgNames;
        this.resName = resName;

    }


    @Override
    public JenaGraphOrBindings eval(List<JenaGraphOrBindings> datasets) {
        JenaGraphOrBindings ds1 = datasets.get(0);
        JenaGraphOrBindings ds2 = datasets.get(1);

        if (query == null || query.isEmpty()) {
            JenaGraphOrBindings res = new JenaGraphOrBindings();
            List<Binding> resBinding = new ArrayList<>();
            resBinding.addAll(ds1.getResult());
            resBinding.addAll(ds2.getResult());
            res.setResult(resBinding);
            //  res.setContent(new Union(ds1.getContent(), ds2.getContent()));
            return res;
        } else {
            if (ds1.getContent() == null || ds2.getContent() == null) {

                Graph g1 = ds1.getContent() == null ? ds2.getContent() : ds1.getContent();
                List<Binding> bs = ds1.getResult() == null ? ds2.getResult() : ds1.getResult();

                Query q = QueryFactory.create(query);

                DatasetGraph dg = new DatasetGraphInMemory();
                Node g0 = NodeFactory.createURI("g0");
                dg.addGraph(g0, g1);

                QueryExecution queryExecution = QueryExecutionFactory.create(q, DatasetImpl.wrap(dg));
                ResultSet resultSet = queryExecution.execSelect();

                List<Binding> res = new ArrayList<>();
                res.addAll(bs);

                while (resultSet.hasNext()) {

                    ResultBinding rb = (ResultBinding) resultSet.next();
                    res.add(rb.getBinding());
                }

                JenaGraphOrBindings bindings = new JenaGraphOrBindings();
                bindings.setResult(res);
                return bindings;

            } else {
                JenaGraphOrBindings dataset = ds1;
                Query q = QueryFactory.create(query);
                DatasetGraph dg = new DatasetGraphInMemory();
                Node g0 = NodeFactory.createURI("g0");
                dg.addGraph(g0, dataset.getContent());

                q.getProjectVars();
                Node g1 = NodeFactory.createURI("g1");
                dg.addGraph(g1, ds2.getContent());

                QueryExecution queryExecution = QueryExecutionFactory.create(q, DatasetImpl.wrap(dg));
                ResultSet resultSet = queryExecution.execSelect();

                List<Binding> res = new ArrayList<>();

                while (resultSet.hasNext()) {

                    ResultBinding rb = (ResultBinding) resultSet.next();
                    res.add(rb.getBinding());

                }


                JenaGraphOrBindings bindings = new JenaGraphOrBindings(new Union(ds1.getContent(), ds2.getContent()));
                bindings.setResult(res);
                return bindings;
            }
        }
    }


    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName;
    }
}
