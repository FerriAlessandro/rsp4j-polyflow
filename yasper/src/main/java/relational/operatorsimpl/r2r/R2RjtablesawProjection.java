/*
package relational.operatorsimpl.r2r;

import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMapping;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import tech.tablesaw.api.Table;

import java.util.Collection;
import java.util.List;

public class R2RjtablesawProjection implements RelationToRelationOperator<Table> {

    CustomRelationalQuery query;

    private List<String> tvgNames;

    private boolean isBinary;

    private String unaryOpName;
    private String binaryOpName;

    public R2RjtablesawProjection(CustomRelationalQuery query, List<String> tvgNames, String unaryOpName, String binaryOpName){
        this.query = query;
        this.tvgNames = tvgNames;
        this.isBinary = false;
        this.unaryOpName = unaryOpName;
        this.binaryOpName = binaryOpName;
    }
    @Override
    public Table evalUnary(Table dataset) {
        if(dataset.isEmpty())
            return dataset;
        return dataset.selectColumns(query.projectionColumns);
    }

    @Override
    public Table evalBinary(Table dataset1, Table dataset2) {
        return null;
    }

    @Override
    public TimeVarying<Collection<Table>> apply(SDS<Table> sds) {
        return null;
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
    public SolutionMapping<Table> createSolutionMapping(Table result) {
        return null;
    }
}
*/
