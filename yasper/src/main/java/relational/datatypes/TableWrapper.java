package relational.datatypes;

import org.javatuples.Tuple;
import tech.tablesaw.api.*;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class TableWrapper {

    Table table = Table.create();

    public TableWrapper(Collection<Tuple> rows){

        if(rows.isEmpty())
            return;

        Tuple t = rows.iterator().next();
        for(int i = 0; i< t.getSize(); i++){
            int idx = i;
            if(t.getValue(i) instanceof Long){
                List<Long> column = rows.stream().map(row -> (Long) row.getValue(idx)).collect(Collectors.toList());
                LongColumn c = LongColumn.create("c"+(i+1));
                column.forEach(l -> c.append(l));
                table.addColumns(c);
            }
            else if(t.getValue(i) instanceof Integer){
                List<Integer> column = rows.stream().map(row -> (Integer) row.getValue(idx)).collect(Collectors.toList());
                IntColumn c = IntColumn.create("c"+(i+1));
                column.forEach(l -> c.append(l));
                table.addColumns(c);
            }
            else if(t.getValue(i) instanceof Boolean){
                List<Boolean> column = rows.stream().map(row -> (Boolean) row.getValue(idx)).collect(Collectors.toList());
                BooleanColumn c = BooleanColumn.create("c"+(i+1));
                column.forEach(l -> c.append(l));
                table.addColumns(c);
            }
            else if(t.getValue(i) instanceof String){
                List<String> column = rows.stream().map(row -> (String) row.getValue(idx)).collect(Collectors.toList());
                StringColumn c = StringColumn.create("c"+(i+1));
                column.forEach(l -> c.append(l));
                table.addColumns(c);
            }

        }
    }

}
