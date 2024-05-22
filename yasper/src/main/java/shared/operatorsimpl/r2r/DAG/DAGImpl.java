package shared.operatorsimpl.r2r.DAG;

import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2r.DAG.DAG;
import org.streamreasoning.rsp4j.api.operators.r2r.DAG.DAGNode;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.*;

public class DAGImpl<R extends Iterable<?>> implements DAG<R> {


    Map<String, DAGNode<R>> root = new HashMap<>();

    DAGNode<R> tail;



   @Override
   public void addToDAG(RelationToRelationOperator<R> op) {

       DAGNode<R> dagNode;
       if(op.getTvgNames().size()>1) //Binary R2R
            dagNode = new BinaryDAGNodeImpl<>(op);

       else //Unary R2R
            dagNode = new UnaryDAGNodeImpl<>(op);

       root.put(op.getResName(), dagNode);
       for(String prev : op.getTvgNames()){
           DAGNode<R> node = root.get(prev);
           node.setNext(dagNode);
           dagNode.addPrev(node);
       }

   }

    public void addTVGs(Collection<TimeVarying<R>> sds){
        for(TimeVarying<R> tvg : sds){
            root.put(tvg.iri(), new DAGRootNodeImpl<>(tvg));
        }
    }

    @Override
    public void initialize(){
        DAGNode<R> tmp  = root.values().stream().findFirst().get();
        while(tmp.hasNext()){
            tmp = tmp.getNext();
        }
        this.tail = tmp;
    }

    @Override
    public R eval(long ts){
        return this.tail.eval(ts);
    }

    @Override
    public TimeVarying<R> apply(){ return this.tail.apply();}

    @Override
    public DAGNode<R> getTail(){
       return this.tail;
    }

    @Override
    public void printDAG(){
       /* Set<DAGNode<R>> printed = new HashSet<>();
        for(DAGNode<R> node : root.values()){
            while(node != null){
                if(!printed.contains(node)) {
                    printed.add(node);
                    System.out.print("[T:" + node.getOperandsNames() + " O:" + node.getOpName() + "]->");
                    node = node.getNext();
                }
                else if(printed.contains(node) && node.isBinary()){
                    System.out.print("[T:" + node.getOperandsNames() + " O:" + node.getOpName() + "]->");
                    break;
                }
            }
            System.out.print("\n");

        }*/
    }
}
