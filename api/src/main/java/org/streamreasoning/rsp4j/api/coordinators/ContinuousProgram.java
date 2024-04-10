package org.streamreasoning.rsp4j.api.coordinators;

import org.streamreasoning.rsp4j.api.operators.s2r.Convertible;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.querying.Task;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.*;


public class ContinuousProgram<I, W extends Convertible<R>, R extends Iterable<?>, O> implements ContinuousProgramInterface<I, W, R, O>, Consumer<I> {

    List<Task<I, W, R, O>> taskList;
    Map<DataStream<I>, List<Task<I, W, R, O>>> registeredTasks;
    Map<Task<I, W, R, O>, List<DataStream<O>>> taskToOutMap;


    public ContinuousProgram(){
        this.taskList = new ArrayList<>();
        this.registeredTasks = new HashMap<>();
        this.taskToOutMap = new HashMap<>();
    }

    @Override
    public void buildTask(String query) {
        //Parse query and extract operators and streams; add them to a new Task


    }
    public void buildTask(Task<I, W, R, O> task, List<DataStream<I>> inputStreams, List<DataStream<O>> outputStreams){
        this.taskList.add(task);
        inputStreams.forEach(input -> addInputStream(input, task));
        outputStreams.forEach(output -> addOutputStream(output, task));
    }

    /**
     * Maps a task to the inputStream it's interested in and adds the Continuous program as a consumer of the input stream
     * @param inputStream Input stream that the task is interested in
     * @param task Task that consumes from InputStream
     */
    private void addInputStream(DataStream<I> inputStream, Task<I, W, R, O> task) {

        inputStream.addConsumer(this);

        if(!registeredTasks.containsKey(inputStream)){
            registeredTasks.put(inputStream, new ArrayList<>());
        }
        if(!registeredTasks.get(inputStream).contains(task)){
            registeredTasks.get(inputStream).add(task);
        }

    }

    /**
     * Maps a task to the associated output stream
     * @param outputStream The stream that the task will write on
     * @param task The task that will write on the output stream
     */
    private void addOutputStream(DataStream<O> outputStream, Task<I, W, R, O> task){

        if(!taskToOutMap.containsKey(task)){
            taskToOutMap.put(task, new ArrayList<>());
        }
        taskToOutMap.get(task).add(outputStream);

    }

    @Override
    public void notify(DataStream<I> inputStream, I element, long timestamp) {

        for(Task<I, W, R, O> t : registeredTasks.get(inputStream)){
            //elaborateElement will transform R to Collection<O> using the task's r2s operators
           Collection<Collection<O>> result = t.elaborateElement(inputStream, element, timestamp);
            if(!taskToOutMap.containsKey(t)){
                throw new RuntimeException("Task has no associated output stream");
            }
            else{
                //If the element triggered a computation and a result is available, insert it in every interested output stream
                result.forEach(coll -> coll.forEach(o -> taskToOutMap.get(t).forEach(out->out.put(o, timestamp))));

            }
        }

    }

}
