package graph.jena.operatorsimpl.r2r.jena;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.engine.binding.BindingProject;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMapping;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Project implements RelationToRelationOperator<JenaGraphOrBindings> {


    private OpProject proj;

    private List<String> tvgNames;

    private boolean isBinary;

    private String unaryOpName;
    private String binaryOpName;

    public Project(OpProject proj, List<String> tvgNames, boolean isBinary, String unaryOpName, String binaryOpName) {
        this.proj = proj;
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


    public JenaGraphOrBindings evalUnary(JenaGraphOrBindings dataset) {

        dataset.setResult(dataset.getResult().stream().map(b -> new BindingProject(proj.getVars(), b)).collect(Collectors.toList()));
        return dataset;
    }

    public JenaGraphOrBindings evalBinary(JenaGraphOrBindings dataset1, JenaGraphOrBindings dataset2) {

        JenaGraphOrBindings result = new JenaGraphOrBindings();
        result.setResult(Stream.concat(dataset1.getResult().stream(), dataset2.getResult().stream()).collect(Collectors.toList()));

        return result;

    }

    public TimeVarying<Collection<JenaGraphOrBindings>> apply(SDS<JenaGraphOrBindings> sds) {
        return null;
    }

    public SolutionMapping<JenaGraphOrBindings> createSolutionMapping(JenaGraphOrBindings result) {
        return null;
    }
}
