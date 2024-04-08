package operatorsimpl.s2r;

import org.apache.commons.rdf.api.IRI;
import org.apache.log4j.Logger;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.exceptions.OutOfOrderElementException;
import org.streamreasoning.rsp4j.api.operators.s2r.Convertible;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOp;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.WindowImpl;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.tick.Ticker;
import org.streamreasoning.rsp4j.api.secret.tick.secret.TickerFactory;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import sds.TimeVaryingObject;

import java.util.*;
import java.util.stream.Collectors;

public class StreamToRelationOpImpl<I, W extends Convertible<?>> implements StreamToRelationOp<I, W> {

    private static final Logger log = Logger.getLogger(StreamToRelationOpImpl.class);
    protected final Ticker ticker;
    protected Tick tick;
    protected final Time time;
    protected final IRI name;
    protected final ContentFactory<I, W> cf;
    protected ReportGrain grain;
    protected Report report;
    private final long width, slide;
    private Map<Window, Content<I, W>> active_windows;
    private Set<Window> to_evict;
    private long t0;
    private long toi;

    public StreamToRelationOpImpl(Tick tick, Time time, IRI name, ContentFactory<I, W> cf, ReportGrain grain, Report report,
                                  long width, long slide){


        this.tick = tick;
        this.time = time;
        this.name = name;
        this.cf = cf;
        this.grain = grain;
        this.report = report;
        this.width = width;
        this.slide = slide;
        this.active_windows = new HashMap<>();
        this.to_evict = new HashSet<>();
        this.t0 = time.getScope();
        this.toi = 0;
        this.ticker = TickerFactory.tick(tick, this);

    }


    @Override
    public Report report() {
        return report;
    }

    @Override
    public Tick tick() {
        return tick;
    }

    @Override
    public Time time() {
        return time;
    }

    @Override
    public ReportGrain grain() {
        return grain;
    }

    @Override
    public IRI getName() {
        return name;
    }

    @Override
    public boolean named() {
        return name != null;
    }

    /**
     * Returns the content of the last window closed before time t_e. If no such window exists, returns an empty content
     */
    @Override
    public Content<I, W> content(long t_e) {
        Optional<Window> max = active_windows.keySet().stream()
                .filter(w -> w.getO() < t_e && w.getC() <= t_e)
                .max(Comparator.comparingLong(Window::getC));

        if (max.isPresent())
            return active_windows.get(max.get());

        return cf.createEmpty();
    }

    /**
     * Returns the content of all the windows closed before time t_e as a list of contents. If no such windows exist, returns an empty list of contents
     */
    @Override
    public List<Content<I, W>> getContents(long t_e) {
        return active_windows.keySet().stream()
                .filter(w -> w.getO() <= t_e && t_e < w.getC())
                .map(active_windows::get).collect(Collectors.toList());
    }

    /**
     * Creates all the windows that can possibly contain the given timestamp
     */
    private void scope(long t_e){

        long c_sup = (long) Math.ceil(((double) Math.abs(t_e - t0) / (double) slide)) * slide;
        long o_i = c_sup - width;
        log.debug("Calculating the Windows to Open. First one opens at [" + o_i + "] and closes at [" + c_sup + "]");

        do {
            log.debug("Computing Window [" + o_i + "," + (o_i + width) + ") if absent");

            active_windows
                    .computeIfAbsent(new WindowImpl(o_i, o_i + width), x -> cf.create());
            o_i += slide;

        } while (o_i <= t_e);
    }


    @Override
    public void windowing(I arg, long ts) {

        log.debug("Received element (" + arg + "," + ts + ")");
        long t_e = ts;

        if (time.getAppTime() > t_e) {
            log.error("OUT OF ORDER NOT HANDLED");
            throw new OutOfOrderElementException("(" + arg + "," + ts + ")");
        }

        scope(t_e);

        active_windows.keySet().forEach(
                w -> {
                    log.debug("Processing Window [" + w.getO() + "," + w.getC() + ") for element (" + arg + "," + ts + ")");
                    if (w.getO() <= t_e && t_e < w.getC()) {
                        log.debug("Adding element [" + arg + "] to Window [" + w.getO() + "," + w.getC() + ")");
                        active_windows.get(w).add(arg);
                    } else if (t_e > w.getC()) {
                        log.debug("Scheduling for Eviction [" + w.getO() + "," + w.getC() + ")");
                        schedule_for_eviction(w);
                    }
                });

        //TODO: here if no window matches the report clause, the app time does not advance since it's set in method tick -> compute
        active_windows.keySet().stream()
                .filter(w -> report.report(w, getWindowContent(w), t_e, System.currentTimeMillis()))
                .max(Comparator.comparingLong(Window::getC))
                .ifPresent(window -> ticker.tick(t_e, window));


    }



    @Override
    public Content<I, W> compute(long t_e, Window w) {
        Content<I, W> content = getWindowContent(w);
        time.setAppTime(t_e);
        log.debug("Report [" + w.getO() + "," + w.getC() + ") with Content " + content + "");
        return content;
    }

    /*@Override
    public StreamToRelationOp<I, W> link(ContinuousQueryExecution<I, W, ?, ?> context) {
        return null;
    }*/

    //TODO: Get and apply do the same thing
    @Override
    public TimeVarying<W> apply() {
        return new TimeVaryingObject(this, name);
    }

    @Override
    public TimeVarying<W> get() {
        return new TimeVaryingObject(this, name);
    }

    private Content<I, W> getWindowContent(Window w) {
        return active_windows.containsKey(w) ? active_windows.get(w) : cf.createEmpty();
    }

    private void schedule_for_eviction(Window w) {
        to_evict.add(w);
    }

    @Override
    public void evict(){
        to_evict.forEach(w -> {
            log.debug("Evicting [" + w.getO() + "," + w.getC() + ")");
            active_windows.remove(w);
            if (toi < w.getC())
                toi = w.getC() + slide;
        });
        to_evict.clear();
    }



}
