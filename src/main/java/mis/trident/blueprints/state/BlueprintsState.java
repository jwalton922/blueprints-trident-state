/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import backtype.storm.task.IMetricsContext;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import mis.track.data.generator.Position;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

/**
 *
 * @author jwalton
 */
public class BlueprintsState<T> implements IBackingMap<T>, Serializable, State {

    private Class<T> type;
    private SerializableMongoDBGraph graph;
    private String host;
    private int port;
    public static String TRANSACTION_ID = "TRANSACTION_ID";
    public static String TRIDENT_VALUE = "TRIDENT_VALUE";
    public static String PREVIOUS_TRIDENT_VALUE = "PREVIOUS_TRIDENT_VALUE";
    public static String CURRENT_TRIDENT_VALUE = "CURRENT_TRIDENT_VALUE";

    public static class Options implements Serializable {

        int localCacheSize = 1000;
        String globalKey = "$GLOBAL$";
    }

    public static <T> StateFactory opaque(SerializableMongoDBGraph graph, Class<T> entityClass) {
        return opaque(graph, entityClass, new Options());
    }

    public static <T> StateFactory opaque(SerializableMongoDBGraph graph, Class<T> entityClass, Options opts) {
        return new Factory<T>(graph, StateType.OPAQUE, entityClass, opts);
    }

    public static <T> StateFactory transactional(SerializableMongoDBGraph graph, Class<T> entityClass) {
        return transactional(graph, entityClass, new Options());
    }

    public static <T> StateFactory transactional(SerializableMongoDBGraph graph, Class<T> entityClass, Options opts) {
        return new Factory<T>(graph, StateType.TRANSACTIONAL, entityClass, opts);
    }

    public static <T> StateFactory nonTransactional(SerializableMongoDBGraph graph, Class<T> entityClass) {
        return nonTransactional(graph, entityClass, new Options());
    }

    public static <T> StateFactory nonTransactional(SerializableMongoDBGraph graph, Class<T> entityClass, Options opts) {
        return new Factory<T>(graph, StateType.NON_TRANSACTIONAL, entityClass, opts);
    }

    protected static class Factory<T> implements StateFactory {

        private final StateType type;
        private final SerializableMongoDBGraph graph;
        private final Class<T> entityClass;
        private final Options opts;

        public Factory(SerializableMongoDBGraph graph, StateType type, Class<T> entityClass, Options opts) {
            this.type = type;
            this.graph = graph;
            this.entityClass = entityClass;
            this.opts = opts;
        }

        @Override
        public State makeState(Map conf, IMetricsContext context, int partitionIndex, int numPartitions) {
            MapState<T> mapState;
            switch (type) {
                case NON_TRANSACTIONAL:
                    mapState = buildNonTransactional();
                    break;
                case TRANSACTIONAL:
                    mapState = buildTransactional();
                    break;
                case OPAQUE:
                    mapState = buildOpaque();
                    break;
                default:
                    throw new RuntimeException("Unknown state type: " + type);
            }

            return new SnapshottableMap<T>(mapState, Arrays.<Object>asList(opts.globalKey));
        }

        private MapState<T> buildTransactional() {
            BlueprintsState<TransactionalValue> state = new BlueprintsState<TransactionalValue>(graph, TransactionalValue.class);
            CachedMap<TransactionalValue> cachedMap = new CachedMap<TransactionalValue>(state, opts.localCacheSize);
            return TransactionalMap.build(cachedMap);
        }

        private MapState<T> buildOpaque() {
            BlueprintsState<OpaqueValue> state = new BlueprintsState<OpaqueValue>(graph, OpaqueValue.class);
            CachedMap<OpaqueValue> cachedMap = new CachedMap<OpaqueValue>(state, opts.localCacheSize);
            return OpaqueMap.build(cachedMap);
        }

        private MapState<T> buildNonTransactional() {
            BlueprintsState<T> state = new BlueprintsState<T>(graph, entityClass);
            CachedMap<T> cachedMap = new CachedMap<T>(state, opts.localCacheSize);
            return NonTransactionalMap.build(cachedMap);
        }
    }

    protected BlueprintsState(SerializableMongoDBGraph graph, Class<T> type) {
        this.graph = graph;
        this.type = type;
    }

//    @Override
//    public List<T> multiGet(List<List<Object>> keys) {
//        System.out.println("Multiget called in Blueprints State");
//        List<T> returns = new ArrayList<T>(keys.size());
//
//        for (List<Object> key : keys) {
//            int count = 0;
////            Map<String, Object> searchParams = new HashMap<String, Object>();
//            GraphQuery query = graph.getGraph().query();
//            for (Object keyObject : key) {
////                System.out.println("Searching for " + keyObject.toString());
//                if (keyObject instanceof Map) {
////                    System.out.println("Key object is a map");
//                    Map<String, Object> keyObjectMap = (Map) keyObject;
//                    for (String mapKey : keyObjectMap.keySet()) {
//                        System.out.println("Adding this to query: "+mapKey+" = "+ keyObjectMap.get(mapKey));
//                        query.has(mapKey, keyObjectMap.get(mapKey));
//                    }
//                } else {
//                    searchParams.put(keyObject.toString(), true);
//                    query.has(keyObject.toString(), Boolean.TRUE);
//                }
//                System.out.println("Performing query");
//                Iterable<Vertex> vertices = query.vertices();
//
//                for (Vertex v : vertices) {
//                    count++;
//                    if (TransactionalValue.class.equals(type)) {
//                        Long tId = v.getProperty(TRANSACTION_ID);
//                        T value = v.getProperty(TRIDENT_VALUE);
//                        T transValue = (T) new TransactionalValue<T>(tId, value);
//                        returns.add(transValue);
//                    } else if (OpaqueValue.class.equals(type)) {
//                        Long tId = v.getProperty(TRANSACTION_ID);
//                        T value = v.getProperty(TRIDENT_VALUE);
//                        T opaqueValue = (T) new OpaqueValue<T>(tId, value);
//                        returns.add(opaqueValue);
//                    } else {
//                        returns.add((T) v.getProperty(TRIDENT_VALUE));
//                    }
//                }
//                System.out.println("Found "+count+" results");
//            }
//            if (count == 0) {
//                returns.add(null);
//            } else {
//                System.out.println("Non null value being returned!");
//            }
//
//        }
//
//        System.out.println("Returning " + returns.size() + " objects for multiget");
//        return returns;
//    }
    public List<T> multiGetOld(List<List<Object>> keys) {
        System.out.println("Multiget called in Blueprints State. Arg size = " + keys.size());
        List<T> returns = new ArrayList<T>(keys.size());
//        Set<String> objects = new HashSet<String>();
        Map<String, T> objectMap = new HashMap<String, T>();
        for (List<Object> key : keys) {
            int count = 0;
//            Map<String, Object> searchParams = new HashMap<String, Object>();

            String identifier = "";
            System.out.println("Inner key size: " + key.size());
            for (Object keyObject : key) {
                System.out.println("2Searching for " + keyObject.toString());
            }
            for (Object keyObject : key) {
                GraphQuery query = graph.getGraph().query();
//                System.out.println("Searching for " + keyObject.toString());

                if (keyObject instanceof Map) {
                    System.out.println("Key object is a map");
                    Map<String, Object> keyObjectMap = (Map) keyObject;
                    identifier = (String) keyObjectMap.get("OBJECT_IDENTIFIER");
                    query.has("OBJECT_IDENTIFIER", identifier);

                    System.out.println("Searching for OBJECT_IDENTIFIER = " + identifier);
                } else {
                    query.has("OBJECT_IDENTIFIER", key.toString());
                }
//                System.out.println("Performing query");
                Iterable<Vertex> vertices = query.vertices();


                for (Vertex v : vertices) {
                    count++;
                    for (String vKey : v.getPropertyKeys()) {
                        System.out.println("Found vertex prop: " + vKey + " = " + v.getProperty(vKey));
                    }
                    if (TransactionalValue.class.equals(type)) {
                        Long tId = v.getProperty(TRANSACTION_ID);
                        T value = v.getProperty(TRIDENT_VALUE);
                        if (count <= 1) {
                            System.out.println("Found transactional value: " + value.toString() + " of type: " + value.getClass().getName());
                        }
                        T transValue = (T) new TransactionalValue<T>(tId, value);
                        returns.add(transValue);
                    } else if (OpaqueValue.class.equals(type)) {
                        Long tId = v.getProperty(TRANSACTION_ID);
                        T value = v.getProperty(TRIDENT_VALUE);
                        T opaqueValue = (T) new OpaqueValue<T>(tId, value);
                        returns.add(opaqueValue);
                    } else {
                        if (!objectMap.containsKey(v.getProperty("OBJECT_IDENTIFIER").toString())) {
                            T object = (T) v.getProperty(TRIDENT_VALUE);
                            returns.add(object);
                            objectMap.put(v.getProperty("OBJECT_IDENTIFIER").toString(), object);
                        } else {
                            System.out.println("Already loaded object");
                            returns.add(objectMap.get(v.getProperty("OBJECT_IDENTIFIER").toString()));
                        }

                    }
                }

//                System.out.println("Found " + count + " results");
            }
            if (objectMap.get(identifier) == null) {
                objectMap.put(identifier, (T) new HashMap<String, Object>());
            }
            if (count == 0) {
                returns.add(objectMap.get(identifier));
            } else {
//                System.out.println("Non null value being returned!");
            }

        }

//        System.out.println("Returning " + returns.size() + " objects for multiget");
        return returns;
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        System.out.println("Multiget called in Blueprints State. Arg size = " + keys.size());
        List<T> returns = new ArrayList<T>(keys.size());
//        Set<String> objects = new HashSet<String>();
        Map<String, List<T>> objectMap = new HashMap<String, List<T>>();
        for (List<Object> keyList : keys) {
            int count = 0;
//            Map<String, Object> searchParams = new HashMap<String, Object>();
            GraphQuery query = graph.getGraph().query();
            //System.out.println("Inner key size: " + keyList.size());
            String index = ""; //index holds a string of all query terms so we can return it instead
            for (Object keyObject : keyList) {
                if (keyObject instanceof GroupByField) {
                    GroupByField key = (GroupByField) keyObject;
                    query.has(key.getKey(), key.getValue());
                    index += key.toString();
                } else {
                    System.out.println("Do not know how to create query from object type: " + keyObject.getClass().getName() + " value: " + keyObject.toString());
                }
            }
            //make sure we have some query parameters, otherwise blueprints returns all objects
            if (index.length() == 0) {
                System.out.println("Did not find any index terms to search on. Returning null");
                returns.add(null);
                continue;
            }
            //if already loaded object from blueprints, return that instead of querying
            if (objectMap.get(index) != null) {
                //System.out.println("Already loaded object for "+index+", skipping db retrieval");
                returns.addAll(objectMap.get(index));
                continue;
            }
            Iterable<Vertex> vertices = query.vertices();
            List<T> foundObjects = new ArrayList<T>();
            for (Vertex v : vertices) {
                count++;
                for (String vKey : v.getPropertyKeys()) {
                    //System.out.println("Found vertex prop: " + vKey + " = " + v.getProperty(vKey));
                }
                if (TransactionalValue.class.equals(type)) {
                    Long tId = v.getProperty(TRANSACTION_ID);
                    T value = v.getProperty(TRIDENT_VALUE);
                    if (count <= 1) {
                        System.out.println("Found transactional value: " + value.toString() + " of type: " + value.getClass().getName());
                    }
                    T transValue = (T) new TransactionalValue<T>(tId, value);
                    returns.add(transValue);
                } else if (OpaqueValue.class.equals(type)) {
                    Long tId = v.getProperty(TRANSACTION_ID);
                    T value = v.getProperty(TRIDENT_VALUE);
                    T opaqueValue = (T) new OpaqueValue<T>(tId, value);
                    returns.add(opaqueValue);
                } else {
                    T object = (T) v.getProperty(TRIDENT_VALUE);
                    //returns.add(object);
                    foundObjects.add(object);
                    //System.out.println("First load for object: "+index);
                    //objectMap.put(index, object);
                }
            }
            if (count == 0) {
                //System.out.println("Found no object to return for: " + index + "!");
                returns.add(null);
            } else {
                if (foundObjects.size() > 1) {
                    System.out.println("BlueprintsState found " + foundObjects.size() + " blueprints objects"+" index = "+index);
                }
                returns.addAll(foundObjects);
                objectMap.put(index, foundObjects);
            }



        } //end for keyList in keys

//        System.out.println("Returning " + returns.size() + " objects for multiget");
        return returns;
    }

    private void updateVertex(Vertex v, T value) {
        if (TransactionalValue.class.equals(type)) {
            TransactionalValue<T> transactionalValue = (TransactionalValue<T>) value;
            v.setProperty(TRANSACTION_ID, transactionalValue.getTxid());

            value = transactionalValue.getVal();
            //objectToSave.addAttribute(TRIDENT_VALUE, transactionalValue.getVal());

        } else if (OpaqueValue.class.equals(type)) {
            OpaqueValue<T> opaqueValue = (OpaqueValue<T>) value;
            //objectToSave.addAttribute(CURRENT_TRIDENT_VALUE, opaqueValue.getCurr());
            v.setProperty(PREVIOUS_TRIDENT_VALUE, opaqueValue.getPrev());
            v.setProperty(TRANSACTION_ID, opaqueValue.getCurrTxid());
            value = opaqueValue.getCurr();
        }
        v.setProperty(TRIDENT_VALUE, value);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        System.out.println("Multiput called in Blueprints State: " + vals.size() + " values: " + keys.size() + " keys");
        for (int i = 0; i < keys.size(); i++) {
            T value = vals.get(i);
            List<Object> keyList = keys.get(i);
            GraphQuery query = graph.getGraph().query();
            List<GroupByField> indices = new ArrayList<GroupByField>();
            for (int j = 0; j < keyList.size(); j++) {
                Object keyObject = keyList.get(j);
                if (keyObject instanceof GroupByField) {
                    GroupByField key = (GroupByField) keyObject;
                    indices.add(key);
                    query.has(key.getKey(), key.getValue());
                } else {
                    System.out.println("Unknown key type: " + keyObject.getClass().getName() + " value: " + keyObject.toString());
                }
            }

            Iterable<Vertex> vertices = query.vertices();
            boolean objectExists = false;
            for (Vertex vertex : vertices) {
                objectExists = true;
                updateVertex(vertex, value);
            }
            if (!objectExists) {
                Vertex vertex = graph.getGraph().addVertex(null);
                for (int z = 0; z < indices.size(); z++) {
                    vertex.setProperty(indices.get(z).getKey(), indices.get(z).getValue());
                }
                updateVertex(vertex, value);
            }
        }
    }

    public void multiPutOld(List<List<Object>> keys, List<T> vals) {
        System.out.println("Multiput called in Blueprints State: " + vals.size() + " values: " + keys.size() + " keys");
        for (int i = 0; i < keys.size(); i++) {
            List<Object> innerKeys = keys.get(i);
            T value = vals.get(i);
            System.out.println("put inner key size: " + innerKeys.size() + " value: " + value.toString() + " of type: " + value.getClass().getName());
            for (int j = 0; j < innerKeys.size(); j++) {

                Object innerKey = innerKeys.get(j);
                if (innerKey instanceof Map) {
                    Map<String, Object> event = (Map<String, Object>) innerKey;
                    String identifier = (String) event.get("TRACKID");
                    GraphQuery query = graph.getGraph().query();
                    query.has("OBJECT_IDENTIFIER", identifier);

                    Iterable<Vertex> vertices = query.vertices();
                    boolean objectExists = false;
                    for (Vertex vertex : vertices) {
                        objectExists = true;
                        updateVertex(identifier, value, vertex);
                    }
                    if (!objectExists) {
                        Vertex v = graph.getGraph().addVertex(null);
                        updateVertex(identifier, value, v);
                    }


                } else {
                    String identifier = innerKey.toString();
                    GraphQuery query = graph.getGraph().query();
                    query.has("OBJECT_IDENTIFIER", identifier);

                    Iterable<Vertex> vertices = query.vertices();
                    boolean objectExists = false;
                    for (Vertex vertex : vertices) {
                        objectExists = true;
                        updateVertex(identifier, value, vertex);
                    }
                    if (!objectExists) {
                        Vertex v = graph.getGraph().addVertex(null);
                        updateVertex(identifier, value, v);
                    }
                }

            }

        }
    }

    private void updateVertex(String identifier, T value, Vertex v) {
        v.setProperty("OBJECT_IDENTIFIER", identifier);
        if (TransactionalValue.class.equals(type)) {
            TransactionalValue<T> transactionalValue = (TransactionalValue<T>) value;
            v.setProperty(TRANSACTION_ID, transactionalValue.getTxid());

            value = transactionalValue.getVal();
            //objectToSave.addAttribute(TRIDENT_VALUE, transactionalValue.getVal());

        } else if (OpaqueValue.class.equals(type)) {
            OpaqueValue<T> opaqueValue = (OpaqueValue<T>) value;
            //objectToSave.addAttribute(CURRENT_TRIDENT_VALUE, opaqueValue.getCurr());
            v.setProperty(PREVIOUS_TRIDENT_VALUE, opaqueValue.getPrev());
            v.setProperty(TRANSACTION_ID, opaqueValue.getCurrTxid());
            value = opaqueValue.getCurr();
        }
        v.setProperty(TRIDENT_VALUE, value);
    }

    public void beginCommit(Long l) {
        System.out.println("Begin commit called on: " + l);
    }

    public void commit(Long l) {
        System.out.println("Commit called on " + l);
    }

    public List<Vertex> getVertices(String field, String identifier) {
        List<Vertex> vertices = new ArrayList<Vertex>();
        GraphQuery query = this.graph.getGraph().query();
        query.has(field, identifier);
        Iterable<Vertex> iterable = query.vertices();
        for (Vertex v : iterable) {
            vertices.add(v);
        }
        return vertices;
    }

    public Vertex createObject(Map<String, Object> event) {
        Vertex v = this.graph.getGraph().addVertex(null);
        Double lat = (Double) event.get("LATITUDE");
        Double lon = (Double) event.get("LONGITUDE");
        Long time = (Long) event.get("TIME");
        v.setProperty("START_TIME", time);
        v.setProperty("END_TIME", time);
        v.setProperty("TRACK_OF_OBJECT", v);
        List<Position> positions = new ArrayList<Position>();
        positions.add(new Position(lat, lon, time));
        v.setProperty("POSITIONS", positions);

        return v;
    }
}
