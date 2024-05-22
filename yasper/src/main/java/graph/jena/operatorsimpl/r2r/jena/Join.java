package graph.jena.operatorsimpl.r2r.jena;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMapping;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class Join implements RelationToRelationOperator<JenaGraphOrBindings> {


    private List<String> tvgNames;

    private boolean isBinary;

    private String unaryOpName;
    private String binaryOpName;

    public Join(List<String> tvgNames, boolean isBinary, String unaryOpName, String binaryOpName) {
        this.tvgNames = tvgNames;
        this.isBinary = isBinary;
        this.unaryOpName = unaryOpName;
        this.binaryOpName = binaryOpName;

    }

    @Override
    public JenaGraphOrBindings eval(List<JenaGraphOrBindings> datasets) {
        return null;
    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return null;
    }

    public boolean isBinary() {
        return isBinary;
    }

    public String getUnaryOpName() {
        return unaryOpName;
    }

    public String getBinaryOpName() {
        return binaryOpName;
    }

    public JenaGraphOrBindings evalUnary(JenaGraphOrBindings dataset) {
        return dataset;
    }

    public JenaGraphOrBindings eval(JenaGraphOrBindings... datasets) {

        List<Binding> collect = datasets[0].getResult().stream().flatMap(l -> datasets[1].getResult().stream().map(r -> Algebra.merge(l, r)))
                .filter(b -> b != null).collect(Collectors.toList());

        JenaGraphOrBindings result = new JenaGraphOrBindings();
        result.setResult(collect);

        return result;

    }

    public JenaGraphOrBindings evalBinary(JenaGraphOrBindings dataset1, JenaGraphOrBindings dataset2) {

        List<Binding> collect = dataset1.getResult().stream().flatMap(l -> dataset2.getResult().stream().map(r -> Algebra.merge(l, r)))
                .filter(b -> b != null).collect(Collectors.toList());

        JenaGraphOrBindings result = new JenaGraphOrBindings();
        result.setResult(collect);

        return result;

    }

    public TimeVarying<Collection<JenaGraphOrBindings>> apply(SDS<JenaGraphOrBindings> sds) {
        return null;
    }

    public SolutionMapping<JenaGraphOrBindings> createSolutionMapping(JenaGraphOrBindings result) {
        return null;
    }
}
