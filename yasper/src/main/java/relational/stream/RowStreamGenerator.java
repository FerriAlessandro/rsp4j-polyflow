package relational.stream;


import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Tuple;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class RowStreamGenerator {

    private static final Long TIMEOUT = 1000l;
    private final Map<String, DataStream<Tuple>> activeStreams;

    private File f1 = new File(RowStreamGenerator.class.getResource("/relational_stream_items.txt").getPath());
    private File f2 = new File(RowStreamGenerator.class.getResource("/relational_stream_happiness.txt").getPath());
    private Scanner s1, s2;
    private final AtomicBoolean isStreaming;


    public RowStreamGenerator() {
        this.activeStreams = new HashMap<String, DataStream<Tuple>>();
        this.isStreaming = new AtomicBoolean(false);
        try {
            s1 = new Scanner(f1);
            s2 = new Scanner(f2);
        }catch(FileNotFoundException ignored){}
    }


    public DataStream<Tuple> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            RowStream stream = new RowStream(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task = () -> {
                long ts = 0;
                while (this.isStreaming.get() && s1.hasNext() && s2.hasNext()) {
                    long finalTs = ts;
                    activeStreams.entrySet().forEach(e -> generateDataAndAddToStream(e.getValue(), finalTs));
                    ts += 300;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                stopStreaming();

            };


            Thread thread = new Thread(task);
            thread.start();
        }
    }

    private void generateDataAndAddToStream(DataStream<Tuple> stream, long ts) {


        Tuple row;
        String [] items;
        String [] happiness;

        if(stream.getName().equals("http://test/stream1")) {
            items = s1.nextLine().split(",");
            row = new Quartet<>(Long.parseLong(items[0]), items[1], Integer.parseInt(items[2]), Boolean.parseBoolean(items[3]));
        }
        else{
            happiness = s2.nextLine().split(",");
            row = new Pair<>(Long.parseLong(happiness[0]), happiness[1]);
        }
        stream.put(row, ts);
    }


    public void stopStreaming() {
        this.isStreaming.set(false);
    }

}
