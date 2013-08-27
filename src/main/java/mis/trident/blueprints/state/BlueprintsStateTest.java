/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.sjoerdmulder.trident.mongodb.MongoState;
import com.tinkerpop.blueprints.Vertex;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import mis.track.data.generator.City;
import mis.track.data.generator.CityLocationReader;
import mis.track.data.generator.Position;
import mis.track.data.generator.Track;
import mis.track.data.generator.TrackGenerator;
import storm.trident.TridentTopology;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class BlueprintsStateTest {

    public static StormTopology buildTopology(LocalDRPC drpc, StateFactory stateFactory) {
        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);
//        TrackGenerator trackGenerator = new TrackGenerator();
//        CityLocationReader cityReader = new CityLocationReader();
//        Map<String, City> cityMap = cityReader.getCitiesMap();
//        City tokyo = cityMap.get("JP_TOKYO");
//        City la = cityMap.get("US_LOS ANGELES");
//        Track tokyoLa = trackGenerator.generateTrack(tokyo, la, System.currentTimeMillis());
//        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
//        for (Position p : tokyoLa.getPositions()) {
//            Map<String, Object> event = new HashMap<String, Object>();
//            event.put("TRACKID", tokyoLa.getName());
//            event.put("LATITUDE", p.getLat());
//            event.put("LONGITUDE", p.getLon());
//            event.put("TIME", p.getTimestamp());
//            events.add(event);
//        }
//        List<Object> outputs = new ArrayList<Object>();
//        outputs.add(events);
        TrackEventBatchSpout eventSpout = null;
        try {
            eventSpout = new TrackEventBatchSpout(4);
        } catch (Exception e) {
            e.printStackTrace();
        }

        TridentTopology topology = new TridentTopology();
        topology.build();
        StateFactory factory = BlueprintsState.nonTransactional(graph, Map.class);
        
        StateFactory mongoFactory = MongoState.transactional("mongodb://127.0.0.1/test.words", Map.class);
        
        topology.newStream("events", eventSpout).parallelismHint(1)
                .each(new Fields("event"), new ObjectIdentifier(), new Fields("identifier"))
                .groupBy(new Fields("identifier")).persistentAggregate(factory, new Fields("event", "identifier"), new BlueprintsReducerAggregator(), new Fields("event"));
//                partitionPersist(factory, new Fields("event", "identifier"), new BlueprintsStateUpdater());


//        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
//                new Values("the cow jumped over the moon"),
//                new Values("the man went to the store and bought some candy"),
//                new Values("four score and seven years ago"),
//                new Values("how many apples can you eat"),
//                new Values("to be or not to be"));

//        TridentTopology topology = new TridentTopology();
//        topology.build();
//        TridentState wordCounts = topology.newStream("spout1", eventSpout).parallelismHint(1)
//                .each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
//                .persistentAggregate(stateFactory, new Fields("word"), new WordCount(new Seriali.zableMongoDBGraph("localhost", 27017)), new Fields("count"))
//                .parallelismHint(1);

//        topology.newDRPCStream("words", drpc)
//                .each(new Fields("args"), new Split(), new Fields("word"))
//                .groupBy(new Fields("word"))
//                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
//                .each(new Fields("count"), new FilterNull())
//                .aggregate(new Fields("count"), new WordSum(), new Fields("sum"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        SortedMap<String, Charset> charsets = Charset.availableCharsets();

        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);

//        StateFactory stateFactory = BlueprintsState.transactional(graph, HolocronObject.class);
//        Config conf = new Config();
//        conf.setMaxSpoutPending(5);
//        if (args.length == 0) {
//            LocalDRPC drpc = new LocalDRPC();
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("wordCounter", conf, buildTopology(drpc, stateFactory));
//            for (int i = 0; i < 100; i++) {
//                long startDate = System.nanoTime();
//                String result = drpc.execute("words", "to");
//                long endDate = System.nanoTime() - startDate;
//                System.out.println("DRPC RESULT: " + result + " took: " + endDate / 1000000);
//                Thread.sleep(100);
//            }
//            cluster.shutdown();
//        } else {
//            conf.setNumWorkers(3);
//            StormSubmitter.submitTopology(args[0], conf, buildTopology(null, stateFactory));
//        }
    }


}
