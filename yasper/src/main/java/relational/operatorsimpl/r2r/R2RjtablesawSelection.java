package relational.operatorsimpl.r2r;

import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import tech.tablesaw.api.Table;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class R2RjtablesawSelection implements RelationToRelationOperator<Table> {
    CustomRelationalQuery query;

    private List<String> tvgNames;

    private String resName;

    public R2RjtablesawSelection(CustomRelationalQuery query, List<String> tvgNames, String resName) {
        this.query = query;
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName;
    }

    @Override
    public Table eval(List<Table> datasets) {

        Table res = datasets.get(0);
        if (!res.isEmpty())
            return res.where(res.intColumn(query.columnName).isGreaterThan(query.selectionValue));
        return res;
    }


}
