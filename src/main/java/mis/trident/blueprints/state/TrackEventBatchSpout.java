/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import mis.track.data.generator.City;
import mis.track.data.generator.CityLocationReader;
import mis.track.data.generator.Position;
import mis.track.data.generator.Track;
import mis.track.data.generator.TrackGenerator;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

/**
 *
 * @author jwalton
 */
public class TrackEventBatchSpout implements IBatchSpout {

    private int batchSize;
    private TrackGenerator trackGenerator;
    private double[] activityDistribution;
    private double[][] subjectInterestDistribution;
    private Random randomGenerator;
    private String[] sentences;
    private long tweetId = 0;
    private List<String> randomPropertyList = new ArrayList<String>();
    private Map<String, City> cityMap;
    private List<City> cities = new ArrayList<City>();
    private List<Vector<City>> cityPairs = new ArrayList<Vector<City>>();
    private List<TrackPositionEmitter> trackPositionEmitters = new ArrayList<TrackPositionEmitter>();
    public TrackEventBatchSpout() throws IOException {
        this(5);
    }

    public TrackEventBatchSpout(int batchSize) throws IOException {
        this.trackGenerator = new TrackGenerator();
        CityLocationReader cityReader = new CityLocationReader();
        this.cityMap = cityReader.getCitiesMap();
        this.batchSize = batchSize;
        randomPropertyList.add("TYPE 1");
        randomPropertyList.add("TYPE 2");
        randomPropertyList.add("TYPE 3");
    }

    private int generateRandomIndex(int indexLength) {
        int index = (int) (Math.floor(Math.random() * indexLength));

        return index;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(Map conf, TopologyContext context) {
// init
        System.err.println("Open Spout instance");
        this.randomGenerator = new Random();
        cities = new ArrayList<City>();
        cities.add(cityMap.get("JP_TOKYO"));
        cities.add(cityMap.get("US_LOS ANGELES"));
        cities.add(cityMap.get("EG_CAIRO"));
        cities.add(cityMap.get("FR_PARIS"));
        for (int i = 0; i < 1; i++) {
            int firstIndex = generateRandomIndex(cities.size());
            int secondIndex = 0;
            boolean done = false;
            while (!done) {
                secondIndex = generateRandomIndex(cities.size());
                if (secondIndex != firstIndex) {
                    done = true;
                }
            }
            Vector<City> cityPair = new Vector<City>();
            cityPair.add(cities.get(firstIndex));
            cityPair.add(cities.get(secondIndex));
            cityPairs.add(cityPair);
            trackPositionEmitters.add(new TrackPositionEmitter(cityPair));
        }

    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
// emit batchSize fake tweets
        for (int i = 0; i < batchSize; i++) {
            collector.emit(getNextTrackEvent());
        }
    }

    @Override
    public void ack(long batchId) {
// nothing to do here
    }

    @Override
    public void close() {
// nothing to do here
    }

    @Override
    public Map getComponentConfiguration() {
// no particular configuration here
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }

    private Values getNextTrackEvent() {
        int trackIndex = generateRandomIndex(this.trackPositionEmitters.size());
        Map<String,Object> event = this.trackPositionEmitters.get(trackIndex).getEvent();
        return new Values(event);
    }

    /**
     * Code snippet:
     * http://stackoverflow.com/questions/2171074/generating-a-probability-distribution
     * Returns an array of size "n" with probabilities between 0 and 1 such that
     * sum(array) = 1.
     */
    private static double[] getProbabilityDistribution(int n, Random randomGenerator) {
        double a[] = new double[n];
        double s = 0.0d;
        for (int i = 0; i < n; i++) {
            a[i] = 1.0d - randomGenerator.nextDouble();
            a[i] = -1 * Math.log(a[i]);
            s += a[i];
        }
        for (int i = 0; i < n; i++) {
            a[i] /= s;
        }
        return a;
    }

    private static int randomIndex(double[] distribution, Random randomGenerator) {
        double rnd = randomGenerator.nextDouble();
        double accum = 0;
        int index = 0;
        for (; index < distribution.length && accum < rnd; index++, accum += distribution[index - 1])
;
        return index - 1;
    }

    public static void main(String[] args) throws IOException, ParseException {
        TrackEventBatchSpout spout = new TrackEventBatchSpout();
        spout.open(null, null);
        for (int i = 0; i < 30; i++) {
            System.out.println(spout.getNextTrackEvent());
        }
    }

    private class TrackPositionEmitter {

        Track track;
        int positionIndex;
        int trackCreationCount = 0;
        Vector<City> cityPair;

        public TrackPositionEmitter(Vector<City> cityPair) {
            this.cityPair = cityPair;
            this.track = trackGenerator.generateTrack(cityPair.get(0), cityPair.get(1), System.currentTimeMillis());
            Date start = new Date();
            Date end = new Date();
            start.setTime(this.track.getStartTime());
            end.setTime(this.track.getEndTime());
            System.out.println("Generated track for "+track.getName()+" that has "+this.track.getPositions().size()+" positions. start: "+start.toString()+" end: "+end.toString());
        }

        public Map<String, Object> getEvent() {
            Map<String, Object> event = new HashMap<String, Object>();
            List<Position> positions = track.getPositions();
            if (positionIndex >= positions.size()) {
                positionIndex = 0;
                trackCreationCount++;
                this.track = trackGenerator.generateTrack(cityPair.get(0), cityPair.get(1), System.currentTimeMillis());
            }

            Position p = positions.get(positionIndex);
            event.put("TRACKID", track.getName() + "_" + trackCreationCount);
            event.put("TIME", p.getTimestamp());
            event.put("LATITUDE", p.getLat());
            event.put("LONGITUDE", p.getLon());
            
            event.put("RANDOM_PROP", randomPropertyList.get(generateRandomIndex(randomPropertyList.size())));
            positionIndex++;

            return event;
        }
    }
}
