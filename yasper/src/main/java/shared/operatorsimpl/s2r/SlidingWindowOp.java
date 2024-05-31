package shared.operatorsimpl.s2r;

import org.apache.log4j.Logger;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.exceptions.OutOfOrderElementException;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.WindowImpl;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVaryingFactory;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.tick.Ticker;
import org.streamreasoning.rsp4j.api.secret.tick.secret.TickerFactory;
import org.streamreasoning.rsp4j.api.secret.time.Time;

import java.util.*;
import java.util.stream.Collectors;

public class SlidingWindowOp<I, W, R extends Iterable<?>> implements StreamToRelationOperator<I, W, R> {

    private static final Logger log = Logger.getLogger(HoppingWindowOp.class);
    protected final Ticker ticker;
    protected Tick tick;
    protected final Time time;
    protected final String name;
    protected final ContentFactory<I, W, R> cf;
    protected final TimeVaryingFactory<R> tvFactory;
    protected ReportGrain grain;
    protected Report report;
    private final long a;
    private Map<Window, Content<I, W, R>> active_windows;
    private Set<Window> to_evict;
    private Map<I, Long> r_stream;
    private Map<I, Long> d_stream;


    public SlidingWindowOp(Tick tick, Time time, String name, ContentFactory<I, W, R> cf, TimeVaryingFactory<R> tvFactory, ReportGrain grain, Report report,
                           long a) {

        this.tvFactory = tvFactory;
        this.tick = tick;
        this.time = time;
        this.name = name;
        this.cf = cf;
        this.grain = grain;
        this.report = report;
        this.a = a;
        this.active_windows = new HashMap<>();
        this.to_evict = new HashSet<>();
        this.r_stream = new HashMap<>();
        this.d_stream = new HashMap<>();
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
    public String getName() {
        return name;
    }


    @Override
    public TimeVarying<R> get() {
        return tvFactory.create(this, name);
    }


    @Override
    public Content<I, W, R> content(long t_e) {
        Optional<Window> max = active_windows.keySet().stream()
                .filter(w -> w.getO() < t_e && w.getC() < t_e)
                .max(Comparator.comparingLong(Window::getC));

        if (max.isPresent())
            return active_windows.get(max.get());

        return cf.createEmpty();
    }

    @Override
    public List<Content<I, W, R>> getContents(long t_e) {
        return active_windows.keySet().stream()
                .filter(w -> w.getO() < t_e && t_e < w.getC())
                .map(active_windows::get).collect(Collectors.toList());
    }


    private Window scope(long t_e) {
        long o_i = t_e - a;
        log.debug("Calculating the Windows to Open. First one opens at [" + o_i + "] and closes at [" + t_e + "]");
        log.debug("Computing Window [" + o_i + "," + t_e + ") if absent");
        Window w;
        active_windows.computeIfAbsent(w = new WindowImpl(o_i, t_e), window -> cf.create(window));
        return w;
    }

    @Override
    public void windowing(I arg, long ts) {
        log.debug("Received element (" + arg + "," + ts + ")");
        long t_e = ts;

        if (time.getAppTime() > t_e) {
            log.error("OUT OF ORDER NOT HANDLED");
            throw new OutOfOrderElementException("(" + arg + "," + ts + ")");
        }

        Window lastestWindow = scope(t_e);

        //Add data to all overlapping window
        active_windows.keySet().forEach(
                w -> {
                    log.debug("Processing Window [" + w.getO() + "," + w.getC() + ") for element (" + arg + "," + ts + ")");
                    /*[w.getO() ... t_e ... w.get() ) */
                    if (w.getO() <= t_e && t_e < w.getC()) {
                        log.debug("Adding element [" + arg + "] to Window [" + w.getO() + "," + w.getC() + ")");
                        active_windows.get(w).add(arg);
                        /*[w.getO() , w.get() ) -> t_e */
                    } else if (t_e > w.getC()) {
                        log.debug("Scheduling for Eviction [" + w.getO() + "," + w.getC() + ")");
                        schedule_for_eviction(w);
                    } else if (lastestWindow.getO() <= t_e && t_e < lastestWindow.getC()) {
                        //evict all those that are not going to be reported
                        log.debug("Scheduling for Eviction [" + w.getO() + "," + w.getC() + ")");
                        schedule_for_eviction(w);
                    }
                });


        Content<I, W, R> content = active_windows.get(lastestWindow);

//        r_stream.entrySet().stream().filter(ee -> ee.getValue() < lastestWindow.getO()).forEach(ee -> d_stream.put(ee.getKey(), ee.getValue()));
//
//        r_stream.entrySet().stream().filter(ee -> ee.getValue() >= lastestWindow.getO()).map(Map.Entry::getKey).forEach(content::add);
//
//        r_stream.put(arg, ts);
//        content.add(arg);

        if (report.report(lastestWindow, content, t_e, System.currentTimeMillis())) {
            ticker.tick(t_e, lastestWindow);
        }

        //REMOVE ALL THE WINDOWS THAT CONTAIN DSTREAM ELEMENTS
        //Theoretically lastestWindow window has always size 1
//        d_stream.entrySet().forEach(ee -> {
//            log.debug("Evicting [" + ee + "]");
//
//            active_windows.forEach((window, content1) -> {
//                if (window.getO() <= ee.getValue() && window.getC() < ee.getValue())
//                    schedule_for_eviction(window);
//
//            });
//            r_stream.remove(ee);
//        });

    }

    private void schedule_for_eviction(Window w) {
        if (!w.isEvicted()) {
            w.evict();
            to_evict.add(w);
        }
    }

    @Override
    public void evict() {
        to_evict.forEach(active_windows::remove);
        to_evict.clear();
    }
}
