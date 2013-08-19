/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import com.tinkerpop.blueprints.Vertex;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import mis.track.data.generator.Position;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class BlueprintsStateUpdater extends BaseStateUpdater<BlueprintsState> {

    private boolean addEventToObject(Map<String, Object> event, List<Vertex> objects) {
        Long time = (Long) event.get("TIME");
        Double lat = (Double) event.get("LATITUDE");
        Double lon = (Double) event.get("LONGITUDE");
        boolean foundObject = false;
        for (int i = 0; i < objects.size(); i++) {
            Vertex v = objects.get(i);
            Long startTime = v.getProperty("START_TIME");
            Long endTime = v.getProperty("END_TIME");

            long minTime = startTime - (10 * 60 * 1000);
            long maxTime = endTime + (10 * 60 * 1000);
            if (time >= minTime && time <= maxTime) {

                List<Position> positions = v.getProperty("POSITIONS");
                positions.add(new Position(lat, lon, time));
                if(time < startTime){
                    v.setProperty("START_TIME", time);
                } else if(time > endTime){
                    v.setProperty("END_TIME", v);
                }
                foundObject = true;
                break;
            }
        }
        
        return foundObject;
    }

    public void updateState(BlueprintsState state, List<TridentTuple> tuples, TridentCollector tc) {
        System.out.println("BlueprintsStateUpdater.updateState called");
        if (tuples == null && tuples.size() <= 0) {
            return;
        }
        String identifier = tuples.get(0).getString(0);

        List<Vertex> objects = state.getVertices("TRACK_OF_OBJECT", identifier);
        Collections.sort(objects, new VertexTimeComparator());
        Collections.sort(tuples, new TupleTimeComparator());

        for (TridentTuple tuple : tuples) {
            Map<String, Object> event = (Map<String, Object>) tuple.get(1);
            boolean foundObjectToUpdate = addEventToObject(event, objects);
            if(!foundObjectToUpdate){
                Vertex newObject = state.createObject(event);
                objects.add(newObject);
            }
        }

    }

    private class TupleTimeComparator implements Comparator<TridentTuple> {

        public int compare(TridentTuple a, TridentTuple b) {
            Map<String, Object> mapA = (Map<String, Object>) a.get(1);
            Map<String, Object> mapB = (Map<String, Object>) b.get(1);

            Long timeA = (Long) mapA.get("TIME");
            Long timeB = (Long) mapB.get("TIME");

            return timeA.compareTo(timeB);
        }
    }

    private class VertexTimeComparator implements Comparator<Vertex> {

        public int compare(Vertex a, Vertex b) {
            Long aTime = a.getProperty("START_TIME");
            Long bTime = b.getProperty("START_TIME");

            return aTime.compareTo(bTime);

        }
    }
}
