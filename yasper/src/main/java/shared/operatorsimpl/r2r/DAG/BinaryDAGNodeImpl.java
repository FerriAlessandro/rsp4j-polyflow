package shared.operatorsimpl.r2r.DAG;

import org.streamreasoning.rsp4j.api.operators.r2r.DAG.DAGNode;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.LazyTimeVarying;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.ArrayList;
import java.util.List;

public class BinaryDAGNodeImpl<R extends Iterable<?>> implements DAGNode<R> {

    private RelationToRelationOperator<R> r2rOperator;
    private DAGNode<R> next;
    private List<DAGNode<R>> prev;
    private String resName;

    public BinaryDAGNodeImpl(RelationToRelationOperator<R> r2rOperator) {
        this.r2rOperator = r2rOperator;
        this.resName = r2rOperator.getResName();
        this.prev = new ArrayList<>();
    }


    @Override
    public List<String> getOperandsNames() {
        return null;
    }


    @Override
    public RelationToRelationOperator<R> getR2rOperator() {
        return null;
    }

    @Override
    public void setNext(DAGNode<R> next) {
        this.next = next;
    }

    @Override
    public void addPrev(DAGNode<R> prev) {
        this.prev.add(prev);
    }

    @Override
    public DAGNode<R> getNext() {
        return this.next;
    }

    @Override
    public List<DAGNode<R>> getPrev() {
        return this.prev;
    }

    @Override
    public boolean hasNext() {
        return this.next != null;
    }

    @Override
    public boolean hasPrev() {
        return !this.prev.isEmpty();
    }

    @Override
    public R eval(long ts) {
        return this.r2rOperator.eval(List.of(prev.get(0).eval(ts), prev.get(1).eval(ts)));
    }

    @Override
    public TimeVarying<R> apply() {
        return new LazyTimeVarying<>(this);
    }

}
