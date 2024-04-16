package graph.jena.operatorsimpl.r2r;

import graph.jena.datatypes.JenaOperandWrapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.*;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetImpl;
import org.apache.jena.sparql.core.ResultBinding;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMapping;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class R2RJenaImpl implements RelationToRelationOperator<JenaOperandWrapper> {


    private String query;

    private List<String> tvgNames;

    private boolean isBinary;

    private String unaryOpName;
    private String binaryOpName;

    public R2RJenaImpl(String query, List<String> tvgNames, boolean isBinary, String unaryOpName, String binaryOpName) {
        this.query = query;
        this.tvgNames = tvgNames;
        this.isBinary = isBinary;
        this.unaryOpName = unaryOpName;
        this.binaryOpName = binaryOpName;

    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public boolean isBinary() {
        return isBinary;
    }

    @Override
    public String getUnaryOpName() {
        return unaryOpName;
    }

    @Override
    public String getBinaryOpName() {
        return binaryOpName;
    }

    @Override
    public JenaOperandWrapper evalUnary(JenaOperandWrapper dataset) {


        Query q = QueryFactory.create(query);
        q.getProjectVars();
        Node aDefault = NodeFactory.createURI("default");
        DatasetGraph dg = new DatasetGraphInMemory();
        dg.addGraph(aDefault, dataset.getContent().content);

        QueryExecution queryExecution = QueryExecutionFactory.create(q, DatasetImpl.wrap(dg));
        ResultSet resultSet = queryExecution.execSelect();

        List<Binding> res = new ArrayList<>();

        while (resultSet.hasNext()) {

            ResultBinding rb = (ResultBinding) resultSet.next();
            res.add(rb.getBinding());

        }

        dataset.setResult(res);
        return dataset;
    }

    @Override
    public JenaOperandWrapper evalBinary(JenaOperandWrapper dataset1, JenaOperandWrapper dataset2) {

        JenaOperandWrapper result = new JenaOperandWrapper();
        result.setResult(Stream.concat(dataset1.getResult().stream(), dataset2.getResult().stream()).collect(Collectors.toList()));

        return result;

    }

    @Override
    public TimeVarying<Collection<JenaOperandWrapper>> apply(SDS<JenaOperandWrapper> sds) {
        return null;
    }

    @Override
    public SolutionMapping<JenaOperandWrapper> createSolutionMapping(JenaOperandWrapper result) {
        return null;
    }
}
