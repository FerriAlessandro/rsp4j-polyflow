package relational.examples;

import org.javatuples.Tuple;
import org.streamreasoning.rsp4j.api.coordinators.ContinuousProgram;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.querying.LazyTaskImpl;
import org.streamreasoning.rsp4j.api.querying.Task;
import org.streamreasoning.rsp4j.api.querying.TaskImpl;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVaryingFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import relational.operatorsimpl.r2r.CustomRelationalQuery;
import relational.operatorsimpl.r2r.R2RjtablesawJoin;
import relational.operatorsimpl.r2r.R2RjtablesawSelection;
import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.sds.TimeVaryingFactoryjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import shared.contentimpl.factories.AccumulatorContentFactory;
import shared.operatorsimpl.r2r.DAG.DAGImpl;
import shared.operatorsimpl.s2r.CSPARQLStreamToRelationOpImpl;
import tech.tablesaw.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class polyflow_LazyEvaluation {

    public static void main(String[] args) throws InterruptedException {

        RowStreamGenerator generator = new RowStreamGenerator();

        DataStream<Tuple> inputStream_1 = generator.getStream("http://test/stream1");
        DataStream<Tuple> inputStream_2 = generator.getStream("http://test/stream2");
        // define output stream
        DataStream<Tuple> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        Tick tick = Tick.TIME_DRIVEN;
        ReportGrain report_grain = ReportGrain.SINGLE;
        Time instance = new TimeImpl(0);
        Time instance_2 = new TimeImpl(0);
        Table emptyContent = Table.create();

        AccumulatorContentFactory<Tuple, Tuple, Table> accumulatorContentFactory = new AccumulatorContentFactory<>(
                t -> t,
                (t) -> {
                    Table r = Table.create();

                    for (int i = 0; i < t.getSize(); i++) {
                        if (t.getValue(i) instanceof Long) {
                            String columnName = "c" + (i + 1);
                            if (!r.containsColumn(columnName)) {
                                LongColumn lc = LongColumn.create(columnName);
                                lc.append((Long) t.getValue(i));
                                r.addColumns(lc);
                            } else {
                                LongColumn lc = (LongColumn) r.column(columnName);
                                lc.append((Long) t.getValue(i));
                            }

                        } else if (t.getValue(i) instanceof Integer) {
                            String columnName = "c" + (i + 1);
                            if (!r.containsColumn(columnName)) {
                                IntColumn lc = IntColumn.create(columnName);
                                lc.append((Integer) t.getValue(i));
                                r.addColumns(lc);
                            } else {
                                IntColumn lc = (IntColumn) r.column(columnName);
                                lc.append((Integer) t.getValue(i));
                            }
                        } else if (t.getValue(i) instanceof Boolean) {
                            String columnName = "c" + (i + 1);
                            if (!r.containsColumn(columnName)) {
                                BooleanColumn lc = BooleanColumn.create(columnName);
                                lc.append((Boolean) t.getValue(i));
                                r.addColumns(lc);
                            } else {
                                BooleanColumn lc = (BooleanColumn) r.column(columnName);
                                lc.append((Boolean) t.getValue(i));
                            }
                        } else if (t.getValue(i) instanceof String) {
                            String columnName = "c" + (i + 1);
                            if (!r.containsColumn(columnName)) {
                                StringColumn lc = StringColumn.create(columnName);
                                lc.append((String) t.getValue(i));
                                r.addColumns(lc);
                            } else {
                                StringColumn lc = (StringColumn) r.column(columnName);
                                lc.append((String) t.getValue(i));
                            }
                        }
                    }
                    return r;
                },
                (r1, r2) -> r1.isEmpty() ? r2 : r1.append(r2),
                emptyContent

        );

        TimeVaryingFactory<Table> tvFactory = new TimeVaryingFactoryjtablesaw<>();

        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ContinuousProgram<>();

        CSPARQLStreamToRelationOpImpl<Tuple, Tuple, Table> s2rOp_1 =
                new CSPARQLStreamToRelationOpImpl<>(
                        tick,
                        instance,
                        "w1",
                        accumulatorContentFactory,
                        tvFactory,
                        report_grain,
                        report,
                        1000,
                        1000);
        CSPARQLStreamToRelationOpImpl<Tuple, Tuple, Table> s2rOp_2 =
                new CSPARQLStreamToRelationOpImpl<>(
                        tick,
                        instance_2,
                        "w2",
                        accumulatorContentFactory,
                        tvFactory,
                        report_grain,
                        report,
                        1000,
                        1000);

        String materializedViewName = "materialized";
        List<String> s2r_names = new ArrayList<>();
        s2r_names.add(materializedViewName);
        s2r_names.add(s2rOp_2.getName());

        CustomRelationalQuery selection = new CustomRelationalQuery(4, "c3");
        CustomRelationalQuery join = new CustomRelationalQuery("c1");

        RelationToRelationOperator<Table> r2rOp = new R2RjtablesawSelection(selection, Collections.singletonList(s2rOp_1.getName()), "partial_1");
        RelationToRelationOperator<Table> r2rBinaryOp = new R2RjtablesawJoin(join, s2r_names, "partial_2");

        RelationToStreamOperator<Table, Tuple> r2sOp = new RelationToStreamjtablesawImpl();


        Task<Tuple, Tuple, Table, Tuple> materializedView = new LazyTaskImpl<>();
        materializedView = materializedView
                .addS2ROperator(s2rOp_1, inputStream_1)
                .addR2ROperator(r2rOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance);
        materializedView.initialize();

        Task<Tuple, Tuple, Table, Tuple> task = new TaskImpl<>();
        task = task
                .addS2ROperator(s2rOp_2, inputStream_2)
                .addR2ROperator(r2rBinaryOp)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance_2);

        //Add the materialized view to the interested task

        TimeVarying<Table> view = materializedView.apply();
        view.setIri(materializedViewName);
        task.getSDS().add(view);

        task.initialize();

        List<DataStream<Tuple>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStream_1);
        inputStreams.add(inputStream_2);


        List<DataStream<Tuple>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);

        cp.buildView(materializedView, Collections.singletonList(inputStream_1));
        cp.buildTask(task, Collections.singletonList(inputStream_2), outputStreams);

        outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

        generator.startStreaming();
        //Thread.sleep(20_000);
        //generator.stopStreaming();

//        cp.notify(inputStream_1, null, System.currentTimeMillis());

        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    System.out.println("going to sleep");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("awake");

                view.materialize(System.currentTimeMillis());
                Table rows1 = view.get();
                rows1.forEach(System.out::println);

                try {
                    System.out.println("going to sleep");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("awake");

                RelationToRelationOperator<Table> r2rOp2 = new R2RjtablesawSelection(selection, Collections.singletonList(s2rOp_1.getName()), "ricstuff");

                TimeVarying<Table> apply = r2rOp2.apply(view);

                apply.materialize(System.currentTimeMillis());
                Table rows = apply.get();
                rows.forEach(System.out::println);


            }
        }).start();

    }


}
