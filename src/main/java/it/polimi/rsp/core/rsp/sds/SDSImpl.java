package it.polimi.rsp.core.rsp.sds;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;
import it.polimi.rsp.core.enums.Entailment;
import it.polimi.rsp.core.enums.Maintenance;
import it.polimi.rsp.core.exceptions.UnregisteredStreamExeception;
import it.polimi.rsp.core.rsp.query.execution.ContinuousQueryExecution;
import it.polimi.rsp.core.rsp.sds.windows.DefaultWindow;
import it.polimi.rsp.core.rsp.sds.windows.NamedWindow;
import it.polimi.rsp.core.rsp.sds.windows.WindowModel;
import it.polimi.rsp.core.rsp.sds.windows.WindowOperator;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.core.DatasetImpl;

import java.util.*;

/**
 * Created by riccardo on 01/07/2017.
 */
@Log4j
public class SDSImpl extends DatasetImpl implements Observer, SDS {

    private final EPServiceProvider cep;
    private Set<ContinuousQueryExecution> observers;
    private final IRIResolver resolver;
    private final String resolvedDefaultStream;
    protected Model knowledge_base;
    protected Model tbox;
    @Getter
    protected Reasoner reasoner;
    @Setter
    @Getter
    private Maintenance maintenanceType;
    @Setter
    @Getter
    private Entailment ontoLang;
    private org.apache.jena.query.Query q;
    private Set<String> defaultWindowStreamNames;
    private Map<String, String> namedWindowStreamNames;
    private Set<String> statementNames;
    private Set<String> resolvedDefaultStreamSet;
    private boolean global_tick;


    public SDSImpl(Model tbox_star, Model knowledge_base_star, IRIResolver r, Maintenance maintenanceType, String id_base, EPServiceProvider esp) {
        super(ModelFactory.createDefaultModel());
        this.cep = esp;
        this.observers = new HashSet<>();
        this.resolver = r;
        this.maintenanceType = maintenanceType;
        this.tbox = tbox_star;
        this.knowledge_base = knowledge_base_star;
        this.defaultWindowStreamNames = new HashSet<>();
        this.namedWindowStreamNames = new HashMap<>();
        this.resolvedDefaultStream = resolver.resolveToStringSilent("default");
        this.statementNames = new HashSet<>();
        this.resolvedDefaultStreamSet = new HashSet<>();
        this.resolvedDefaultStreamSet.add(resolvedDefaultStream);
        this.global_tick = false;
    }


    @Override
    public synchronized void update(Observable o, Object _ts) {
        WindowOperator tvg = (WindowOperator) o;
        EPStatement stmt = tvg.getTriggeringStatement();
        long cep_time = tvg.getTimestamp();
        long sys_time = System.currentTimeMillis();

        log.info("[" + Thread.currentThread() + "][" + sys_time + "] " +
                "From Statement [ " + tvg.getTriggeringStatement().getText() + "] at " + cep_time);

        setDefaultModel(getDefaultModel().union(knowledge_base));

        if (global_tick) {
            updateDataset(tvg, cep);
        }

        if (observers != null) {
            for (ContinuousQueryExecution qe : observers) {
                qe.materialize(tvg);
                qe.eval(this, stmt, cep_time);
            }
        }
        setDefaultModel(getDefaultModel().difference(knowledge_base));
    }

    private void updateDataset(WindowOperator tvg, EPServiceProvider esp) {
        EPStatement stmt = tvg.getTriggeringStatement();
        List<EventBean> events = new ArrayList<EventBean>();
        for (String stmtName : statementNames) {
            if (!stmtName.equals(stmt.getName())) {
                EPStatement statement1 = esp.getEPAdministrator().getStatement(stmtName);
                log.debug("[" + System.currentTimeMillis() + "] Polling STATEMENT: " + statement1.getText() + " "
                        + statement1.getTimeLastStateChange());
                SafeIterator<EventBean> it = statement1.safeIterator();
                while (it.hasNext()) {
                    EventBean next = it.next();
                    log.info(next.getUnderlying());
                    events.add(next);
                }

                it.close();
            }
        }


    }

    private String resolveWindowUri(String stream_uri) {

        if (defaultWindowStreamNames.contains(stream_uri)) {
            return resolvedDefaultStream;
        } else if (namedWindowStreamNames.containsKey(stream_uri)) {
            return namedWindowStreamNames.get(stream_uri);
        } else {
            throw new UnregisteredStreamExeception("GraphStream [" + stream_uri + "] is unregistered");
        }
    }

    @Override
    public boolean addDefaultWindowStream(String statementName, String uri) {
        resolvedDefaultStreamSet.add(statementName);
        defaultWindowStreamNames.add(uri);
        return defaultWindowStreamNames.contains(uri);
    }

    @Override
    public boolean addNamedWindowStream(String w, String s, WindowModel model) {
        log.info("Added named window [" + w + "] on stream [" + s + " ]");
        final String uri = resolver.resolveToStringSilent(w);
        addNamedModel(uri, model.getModel());
        if (namedWindowStreamNames.containsKey(s)) {
            return false;
        } else {
            namedWindowStreamNames.put(s, uri);
            return true;
        }
    }

    @Override
    public void addQueryExecutor(ContinuousQueryExecution o) {
        observers.add(o);
    }

    public void addTimeVaryingGraph(DefaultWindow defTVG) {
        defTVG.addObserver(this);
    }

    public void addNamedTimeVaryingGraph(String statementName, String window_uri, String stream_uri, NamedWindow namedTVG) {
        statementNames.add(statementName);
        addNamedWindowStream(window_uri, stream_uri, namedTVG);
        namedTVG.addObserver(this);
    }
}
