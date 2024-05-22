package shared.operatorsimpl.r2r.DAG;

import org.streamreasoning.rsp4j.api.operators.r2r.DAG.DAGNode;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DAGRootNodeImpl<R extends Iterable<?>> implements DAGNode<R>{


    TimeVarying<R> tvg;

    private List<DAGNode<R>> next = new ArrayList<>();


    public DAGRootNodeImpl(TimeVarying<R> tvg){
        this.tvg = tvg;
    }

    @Override
    public List<String> getOperandsNames() {
        return Collections.emptyList();
    }


    @Override
    public RelationToRelationOperator<R> getR2rOperator() {
        return null;
    }

    @Override
    public void setNext(DAGNode<R> next) {
        this.next.add(next);
    }

    @Override
    public void addPrev(DAGNode<R> prev) {
        throw new RuntimeException("Impossible to add a previous node to a dag root");
    }

    @Override
    public DAGNode<R> getNext() {
        return next.get(0);
    }

    @Override
    public List<DAGNode<R>> getPrev() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return !this.next.isEmpty();
    }

    @Override
    public boolean hasPrev() {
        return false;
    }

    @Override
    public R eval(long ts) {
        tvg.materialize(ts);
        return tvg.get();
    }

    @Override
    public TimeVarying<R> apply() {
        throw new RuntimeException("Impossible to apply on a dag node root");
    }

}
