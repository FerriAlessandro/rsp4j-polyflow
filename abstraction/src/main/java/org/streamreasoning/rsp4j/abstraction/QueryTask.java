package org.streamreasoning.rsp4j.abstraction;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.rdf.api.Graph;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.StreamToRelationOperatorFactory;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOp;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.TimeFactory;
import org.streamreasoning.rsp4j.yasper.content.GraphContentFactory;
import org.streamreasoning.rsp4j.yasper.examples.RDFStream;
import org.streamreasoning.rsp4j.yasper.querying.operators.Rstream;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.Binding;
import org.streamreasoning.rsp4j.yasper.querying.operators.windowing.CSPARQLStreamToRelationOp;
import org.streamreasoning.rsp4j.yasper.querying.operators.windowing.CSPARQLTimeWindowOperatorFactory;

public class QueryTask extends Task<Graph,Graph,Binding, Binding>{



    public QueryTask(TaskBuilder<Graph, Graph, Binding, Binding> builder) {
        super(builder);

    }



    public static class QueryTaskBuilder extends TaskBuilder<Graph, Graph, Binding, Binding> {


        public QueryTaskBuilder() {
            super();
        }

        public QueryTaskBuilder fromQuery(ContinuousQuery<Graph, Graph, Binding, Binding> query){
            //add S2R
            Report report = new ReportImpl();
            report.add(new OnWindowClose());

            Tick tick = Tick.TIME_DRIVEN;
            ReportGrain report_grain = ReportGrain.SINGLE;

            int scope = 0;

            //S2R DECLARATION

            query.getWindowMap().entrySet().stream().forEach(entry -> {
                StreamToRelationOp<Graph, Graph> s2r = new CSPARQLStreamToRelationOp<Graph, Graph>(RDFUtils.createIRI(entry.getKey().iri()), entry.getKey().getRange(), entry.getKey().getStep(), TimeFactory.getInstance(), tick, report, report_grain, new GraphContentFactory());
                this.addS2R(entry.getValue().getName(),  s2r,entry.getKey().iri());
            });
            // R2R DECLARATION
            this.addR2R(null,query.r2r()); //TODO restrict TVG
            this.addR2S(query.getOutputStream().getName(),new Rstream<Binding,Binding>());

            return this;
        }



    }


}
