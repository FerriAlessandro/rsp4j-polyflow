package relational.operatorsimpl.r2r;

import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import tech.tablesaw.api.Table;

import java.util.Collections;
import java.util.List;

public class R2RjtablesawJoin implements RelationToRelationOperator<Table> {

    CustomRelationalQuery query;

    private List<String> tvgNames;

    private String resName;

    public R2RjtablesawJoin(CustomRelationalQuery query, List<String> tvgNames, String resName) {
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

        Table dataset1 = datasets.get(0);
        Table dataset2 = datasets.get(1);
        if (dataset1.isEmpty())
            return dataset1;
        if (dataset2.isEmpty())
            return dataset2;
        for (int i = 0; i < dataset2.columnCount(); i++) {
            if (!dataset2.column(i).name().equals(query.columnName)) {
                dataset2.column(i).setName("t2." + dataset2.column(i).name());
            }
        }
        return dataset1.joinOn(query.columnName).inner(dataset2);

    }
}
