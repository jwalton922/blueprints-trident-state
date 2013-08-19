/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class BlueprintsReducerAggregator implements ReducerAggregator<Map<String, Object>> {

    public Map<String, Object> init() {
        System.out.println("Initializing reducer object");
        return new HashMap<String, Object>();
    }

    private boolean addEventToObject(Map<String, Object> event, List<Map<String, Object>> tracks) {
        System.out.println("Checking event against "+tracks.size()+" tracks");
        Long time = (Long) event.get("TIME");
        Double lat = (Double) event.get("LATITUDE");
        Double lon = (Double) event.get("LONGITUDE");
        boolean foundObject = false;
        for (int i = 0; i < tracks.size(); i++) {
            Map<String, Object> t = tracks.get(i);
            Long startTime = (Long) t.get("START_TIME");
            Long endTime = (Long) t.get("END_TIME");

            long minTime = startTime - (10 * 60 * 1000);
            long maxTime = endTime + (10 * 60 * 1000);
            if (time >= minTime && time <= maxTime) {

                List<Map<String, Object>> positions = (List<Map<String, Object>>) t.get("POSITIONS");
                Map<String, Object> position = new HashMap<String, Object>();
                position.put("LATITUDE", lat);
                position.put("LONGITUDE", lon);
                position.put("TIME", time);
                Date date = new Date();
                date.setTime(time);
                position.put("DATE", date.toString());
                positions.add(position);
                t.put("POSITIONS", positions);
                System.out.println(t.get("TRACK_NAME") + " now has " + positions.size() + " positions");
                if (time < startTime) {
                    t.put("START_TIME", time);
                } else if (time > endTime) {
                    t.put("END_TIME", time);
                }
                foundObject = true;
                break;
            }
        }

        return foundObject;
    }

    public Map<String, Object> reduce(Map<String, Object> blueprintsObject, TridentTuple tuple) {
        if (tuple == null) {
            System.out.println("Reduce called on null tuple!");
            return null;
        }
        if (blueprintsObject == null) {
            System.out.println("Reduce called on null blueprints object!");
//            return null;
            blueprintsObject = new HashMap<String, Object>();
        }
        System.out.println("Reduce called on size of tuple: " + tuple.size());
        for (int i = 0; i < tuple.size(); i++) {
            System.out.println("Tuple value at index " + i + ": " + tuple.get(i).toString());
        }
        if (tuple.size() == 0) {
            return blueprintsObject;
        }
        Map<String, Object> event = (Map<String, Object>) tuple.get(0);
        //System.out.println("Reduce called on blueprints obj: " + blueprintsObject.toString() + " event: " + event.toString());
        String identifier = event.get("OBJECT_IDENTIFIER").toString();
        blueprintsObject.put("IDENTIFIER", identifier);
        if (blueprintsObject.get("TRACKS") == null) {
            System.out.println("Creating new list of tracks for: "+identifier);
            blueprintsObject.put("TRACKS", new ArrayList<Map<String, Object>>());
        }
        if(blueprintsObject.get("POSITION_COUNT") == null){
            blueprintsObject.put("POSITION_COUNT", 0);
        }
        Integer positionCount = (Integer) blueprintsObject.get("POSITION_COUNT");
        positionCount++;
        blueprintsObject.put("POSITION_COUNT", positionCount);
        List<Map<String, Object>> tracks = (List<Map<String, Object>>) blueprintsObject.get("TRACKS");
        boolean foundObjectToUpdate = addEventToObject(event, tracks);
        if (!foundObjectToUpdate) {

//            List<Position> positions = new ArrayList<Position>();
            Double lat = (Double) event.get("LATITUDE");
            Double lon = (Double) event.get("LONGITUDE");
            Long time = (Long) event.get("TIME");
            //System.out.println("Start time: " + time);
            System.out.println("Creating new track for object: " + identifier + " Start time: " + time + ". Has " + tracks.size() + " tracks before this new one is created.");
//            positions.add(new Position(lat, lon, time));

//            Track t = new Track("TRACK_OF_"+identifier,positions);
//            t.setStartTime(time);
//            t.setEndTime(time);
            Map<String, Object> track = new HashMap<String, Object>();
            track.put("TRACK_NAME", "TRACK_OF_" + identifier);
            track.put("START_TIME", time);
            track.put("END_TIME", time);
            List<Map<String, Object>> positions = new ArrayList<Map<String, Object>>();
            Map<String, Object> position = new HashMap<String, Object>();
            position.put("LATITUDE", lat);
            position.put("LONGITUDE", lon);
            position.put("TIME", time);
            Date date = new Date();
            date.setTime(time);
            position.put("DATE", date.toString());
            positions.add(position);
            track.put("POSITIONS", positions);
            tracks.add(track);
        }
        return blueprintsObject;
    }
}
