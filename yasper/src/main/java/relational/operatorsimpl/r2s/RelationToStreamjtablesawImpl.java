package relational.operatorsimpl.r2s;

import org.javatuples.*;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class RelationToStreamjtablesawImpl implements RelationToStreamOperator<Table, Tuple> {

    public Tuple transform(Object sm, long ts) {
        return null;
    }

    private List<Tuple> customTransform(Table table){

        List<Tuple> result = new ArrayList<>();
        switch(table.columnCount()){
            case 0:
                break;

            case 1:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Unit<>(
                            table.column(0).get(i)
                    );
                    result.add(t);
                }
                break;

            case 2:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Pair<>(
                            table.column(0).get(i),
                            table.column(1).get(i)
                    );
                    result.add(t);
                }
                break;

            case 3:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Triplet<>(
                            table.column(0).get(i),
                            table.column(1).get(i),
                            table.column(2).get(i)
                    );
                    result.add(t);
                }
                break;

            case 4:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Quartet<>(
                            table.column(0).get(i),
                            table.column(1).get(i),
                            table.column(2).get(i),
                            table.column(3).get(i)
                    );
                    result.add(t);
                }
                break;

            case 5:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Quintet<>(
                            table.column(0).get(i),
                            table.column(1).get(i),
                            table.column(2).get(i),
                            table.column(3).get(i),
                            table.column(4).get(i)
                    );
                    result.add(t);
                }
                break;

            case 6:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Sextet<>(
                            table.column(0).get(i),
                            table.column(1).get(i),
                            table.column(2).get(i),
                            table.column(3).get(i),
                            table.column(4).get(i),
                            table.column(5).get(i)
                    );
                    result.add(t);
                }
                break;

            case 7:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Septet<>(
                            table.column(0).get(i),
                            table.column(1).get(i),
                            table.column(2).get(i),
                            table.column(3).get(i),
                            table.column(4).get(i),
                            table.column(5).get(i),
                            table.column(6).get(i)
                    );
                    result.add(t);
                }
                break;

            case 8:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Octet<>(
                            table.column(0).get(i),
                            table.column(1).get(i),
                            table.column(2).get(i),
                            table.column(3).get(i),
                            table.column(4).get(i),
                            table.column(5).get(i),
                            table.column(6).get(i),
                            table.column(7).get(i)
                    );
                    result.add(t);
                }
                break;

            case 9:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Ennead<>(
                            table.column(0).get(i),
                            table.column(1).get(i),
                            table.column(2).get(i),
                            table.column(3).get(i),
                            table.column(4).get(i),
                            table.column(5).get(i),
                            table.column(6).get(i),
                            table.column(7).get(i),
                            table.column(8).get(i)
                    );
                    result.add(t);
                }
                break;

            case 10:
                for(int i = 0; i< table.rowCount(); i++){
                    Tuple t = new Decade<>(
                            table.column(0).get(i),
                            table.column(1).get(i),
                            table.column(2).get(i),
                            table.column(3).get(i),
                            table.column(4).get(i),
                            table.column(5).get(i),
                            table.column(6).get(i),
                            table.column(7).get(i),
                            table.column(8).get(i),
                            table.column(9).get(i)
                    );
                    result.add(t);
                }
                break;

            default:
                throw new RuntimeException("Too many columns in the result, maximum number supported is 10");
        }


        return result;
    }

    public Stream<Tuple> eval(Table sml, long ts) {

        return customTransform(sml).stream();
    }

}
