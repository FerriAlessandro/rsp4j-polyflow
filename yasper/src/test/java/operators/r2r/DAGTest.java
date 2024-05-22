/*
package operators.r2r;

import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2r.DAG.DAG;
import relational.operatorsimpl.r2r.CustomRelationalQuery;
import operatorsimpl.r2r.DAG.DAGImpl;
import relational.operatorsimpl.r2r.R2RjtablesawImpl;
import tech.tablesaw.api.BooleanColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Table;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DAGTest {

    @Test
    public void Test() {

        DAG<Table> dag = new DAGImpl<>();
        IntColumn c1 = IntColumn.create("intCol");
        for(int i = 0; i<10; i++){
            c1.append(i);
        }
        BooleanColumn c2 = BooleanColumn.create("joinCol");
        for(int i =0; i<10; i++){
            c2.append(i%2==0);
        }

        IntColumn c3 = IntColumn.create("intCol");
        for(int i = 0; i<2; i++){
            c3.append(i);
        }
        BooleanColumn c4 = BooleanColumn.create("joinCol");
        for(int i =0; i<2; i++){
            c4.append(i%2==0);
        }

        Table t1 = Table.create(c1, c2);
        Table t2 = Table.create(c4, c3);
        List<String> tvgNames = new ArrayList<>();
        tvgNames.add("test1");
        tvgNames.add("test2");

        CustomRelationalQuery selection = new CustomRelationalQuery(5, "intCol");
        CustomRelationalQuery join = new CustomRelationalQuery("joinCol");

        RelationToRelationOperator<Table> r2rSelection = new R2RjtablesawImpl(selection, Collections.singletonList("test1"), false, "selection", "empty");
        RelationToRelationOperator<Table> r2rJoin = new R2RjtablesawImpl(join, tvgNames, true, "empty", "join");
        dag.addToDAG(Collections.singletonList("test1"), r2rSelection);
        dag.addToDAG(tvgNames, r2rJoin);
        System.out.println(t1);
        System.out.println(t2);
        dag.eval(tvgNames.get(0), t1);
        Table result = dag.eval(tvgNames.get(1), t2);
        System.out.print(result);
        assertTrue(result.columns().size() == 3);
        assertTrue(result.rowCount() == 4);


    }

}
*/
