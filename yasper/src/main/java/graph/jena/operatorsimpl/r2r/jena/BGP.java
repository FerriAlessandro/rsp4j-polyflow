package graph.jena.operatorsimpl.r2r.jena;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class BGP implements UnaryOperator<JenaGraphOrBindings>, RelationToRelationOperator<JenaGraphOrBindings> {


    private OpBGP bgp;

    private List<String> tvgNames;

    private String unaryOpName;

    public BGP(OpBGP bgp, List<String> tvgNames, String unaryOpName) {
        this.bgp = bgp;
        this.tvgNames = tvgNames;
        this.unaryOpName = unaryOpName;

    }

    public String getUnaryOpName() {
        return unaryOpName;
    }


    public JenaGraphOrBindings eval(JenaGraphOrBindings dataset) {

        QueryIterator exec = Algebra.exec(bgp, dataset.getContent());

        List<Binding> res = new ArrayList<>();

        while (exec.hasNext()) {
            res.add(exec.next());
        }

        dataset.setResult(res);
        return dataset;
    }

    public List<TP> getTPs() {
        List<TP> tps = new ArrayList<>();
        bgp.getPattern().iterator().forEachRemaining(triple -> tps.add(new TP(new OpTriple(triple), tvgNames, unaryOpName)));
        return tps;
    }

    @Override
    public JenaGraphOrBindings apply(JenaGraphOrBindings bindings) {
        return null;
    }

    @Override
    public <V> Function<V, JenaGraphOrBindings> compose(Function<? super V, ? extends JenaGraphOrBindings> before) {
        return UnaryOperator.super.compose(before);
    }

    @Override
    public <V> Function<JenaGraphOrBindings, V> andThen(Function<? super JenaGraphOrBindings, ? extends V> after) {
        return UnaryOperator.super.andThen(after);
    }

    @Override
    public JenaGraphOrBindings eval(List<JenaGraphOrBindings> datasets) {
        return eval(datasets.get(0));
    }

    @Override
    public List<String> getTvgNames() {
        return null;
    }

    @Override
    public String getResName() {
        return null;
    }
}