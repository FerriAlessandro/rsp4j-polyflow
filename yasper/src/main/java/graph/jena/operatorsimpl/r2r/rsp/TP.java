package graph.jena.operatorsimpl.r2r.jena;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

public class TP implements UnaryOperator<JenaGraphOrBindings>, RelationToRelationOperator<JenaGraphOrBindings> {


    private OpBGP bgp;

    private List<String> tvgNames;

    private String unaryOpName;

    public TP(OpBGP bgp, List<String> tvgNames, String unaryOpName) {
        this.bgp = bgp;
        this.tvgNames = tvgNames;
        this.unaryOpName = unaryOpName;

    }

    public JenaGraphOrBindings eval(List<JenaGraphOrBindings> datasets) {
        QueryIterator exec = Algebra.exec(bgp, datasets.get(0).getContent());

        List<Binding> res = new ArrayList<>();

        while (exec.hasNext()) {
            res.add(exec.next());
        }

        JenaGraphOrBindings ds = new JenaGraphOrBindings();
        ds.setResult(res);
        return ds;
    }


    public List<String> getTvgNames() {
        return tvgNames;
    }

    public String getResName() {
        return null;
    }


    public JenaGraphOrBindings eval(JenaGraphOrBindings dataset) {

        QueryIterator exec = Algebra.exec(bgp, dataset.getContent());

        List<Binding> res = new ArrayList<>();

        while (exec.hasNext()) {
            res.add(exec.next());
        }

        JenaGraphOrBindings ds = new JenaGraphOrBindings();
        ds.setResult(res);
        return ds;
    }

    @Override
    public JenaGraphOrBindings apply(JenaGraphOrBindings bindings) {
        return null;
    }

    public Node getProperty() {
        return bgp.getPattern().getList().get(0).getPredicate();
    }

    public Node getObject() {
        return bgp.getPattern().getList().get(0).getObject();
    }
}