package relational.operatorsimpl.r2r;

public class CustomRelationalQuery{

    long selectionValue; //Value used in the selection query "greater than (selectionValue)"
    String columnName; //Name of the join column or the column on which to perform the selection

    int [] projectionColumns; //Column indexes on which to perform the projection

    public CustomRelationalQuery(long selectionValue, String columnName){
        this.selectionValue = selectionValue;
        this.columnName = columnName;
    }
    public CustomRelationalQuery(String columnName){
        this.columnName = columnName;
    }
    public CustomRelationalQuery (int [] projectionColumns){
        this.projectionColumns = projectionColumns;
    }
}
