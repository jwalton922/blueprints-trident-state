/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import backtype.storm.task.IMetricsContext;
import com.tinkerpop.blueprints.Graph;

import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import mis.track.data.generator.Position;
import org.apache.log4j.Logger;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.TransactionalMap;

/**
 *
 * @author jwalton
 */
public class BlueprintsState<T> implements IBackingMap<T>, Serializable, MapState<T> {

    private static Logger log = Logger.getLogger(BlueprintsState.class);
    private Class<T> type;
    private Graph graph;
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

//    public static <T> StateFactory opaque(SerializableMongoDBGraph graph, Class<T> entityClass) {
//        return opaque(graph, entityClass, new Options());
//    }
//
//    public static <T> StateFactory opaque(SerializableMongoDBGraph graph, Class<T> entityClass, Options opts) {
//        return new Factory<T>(graph, StateType.OPAQUE, entityClass, opts);
//    }
//
//    public static <T> StateFactory transactional(SerializableMongoDBGraph graph, Class<T> entityClass) {
//        return transactional(graph, entityClass, new Options());
//    }
//
//    public static <T> StateFactory transactional(SerializableMongoDBGraph graph, Class<T> entityClass, Options opts) {
//        return new Factory<T>(graph, StateType.TRANSACTIONAL, entityClass, opts);
//    }
//
//    public static <T> StateFactory nonTransactional(SerializableMongoDBGraph graph, Class<T> entityClass) {
//        return nonTransactional(graph, entityClass, new Options());
//    }
//
//    public static <T> StateFactory nonTransactional(SerializableMongoDBGraph graph, Class<T> entityClass, Options opts) {
//        return new Factory<T>(graph, StateType.NON_TRANSACTIONAL, entityClass, opts);
//    }

    public static class Factory<T> implements StateFactory {

        private final StateType type;
        private final Graph graph;
        private final Class<T> entityClass;
        private final Options opts;

        public Factory(Graph graph, StateType type, Class<T> entityClass, Options opts) {
            this.type = type;
            this.graph = graph;
            this.entityClass = entityClass;
            this.opts = opts;
        }

        @Override
        public State makeState(Map conf, IMetricsContext context, int partitionIndex, int numPartitions) {
//            MapState<T> mapState;
            BlueprintsState<T> state = new BlueprintsState<T>(graph, entityClass);
//            switch (type) {
//                case NON_TRANSACTIONAL:
//                    mapState = buildNonTransactional();
//
//                    break;
//                case TRANSACTIONAL:
//                    mapState = buildTransactional();
//                    break;
//                case OPAQUE:
//                    mapState = buildOpaque();
//                    break;
//                default:
//                    throw new RuntimeException("Unknown state type: " + type);
//            }

//            return new SnapshottableMap<T>(mapState, Arrays.<Object>asList(opts.globalKey));
            return state;
        }

//        private MapState<T> buildTransactional() {
//            BlueprintsState<TransactionalValue> state = new BlueprintsState<TransactionalValue>(graph, TransactionalValue.class);
//            CachedMap<TransactionalValue> cachedMap = new CachedMap<TransactionalValue>(state, opts.localCacheSize);
//            return TransactionalMap.build(cachedMap);
//        }
//
//        private MapState<T> buildOpaque() {
//            BlueprintsState<OpaqueValue> state = new BlueprintsState<OpaqueValue>(graph, OpaqueValue.class);
//            CachedMap<OpaqueValue> cachedMap = new CachedMap<OpaqueValue>(state, opts.localCacheSize);
//            return OpaqueMap.build(cachedMap);
//        }
//
//        private MapState<T> buildNonTransactional() {
//            BlueprintsState<T> state = new BlueprintsState<T>(graph, entityClass);
//            CachedMap<T> cachedMap = new CachedMap<T>(state, opts.localCacheSize);
//            return NonTransactionalMap.build(cachedMap);
//        }
    }

    protected BlueprintsState(Graph graph, Class<T> type) {
        this.graph = graph;
        this.type = type;
    }

    /**
     *
     * @param keys
     * @return
     */
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        long start = System.currentTimeMillis();
        log.debug("Multiget called in Blueprints State. Arg size = " + keys.size());
        List<T> returns = new ArrayList<T>(keys.size());
//        Set<String> objects = new HashSet<String>();
        Map<String, List<T>> objectMap = new HashMap<String, List<T>>();
        for (List<Object> keyList : keys) {
            int count = 0;
//            Map<String, Object> searchParams = new HashMap<String, Object>();
            GraphQuery query = graph.query();
            //log.debug("Inner key size: " + keyList.size());
            String index = ""; //index holds a string of all query terms so we can return it instead
            for (Object keyObject : keyList) {
                if (keyObject instanceof GroupByField) {
                    GroupByField key = (GroupByField) keyObject;
                    query.has(key.getKey(), key.getValue());
                    index += key.toString();
                } else {
                    log.debug("Do not know how to create query from object type: " + keyObject.getClass().getName() + " value: " + keyObject.toString());
                }
            }
            //make sure we have some query parameters, otherwise blueprints returns all objects
            if (index.length() == 0) {
                log.debug("Did not find any index terms to search on. Returning null");
                returns.add(null);
                continue;
            }
            //if already loaded object from blueprints, return that instead of querying
            if (objectMap.get(index) != null) {
                //log.debug("Already loaded object for "+index+", skipping db retrieval");
                returns.addAll(objectMap.get(index));
                continue;
            }
            Iterable<Vertex> vertices = query.vertices();
            List<T> foundObjects = new ArrayList<T>();
            for (Vertex v : vertices) {
                count++;
                for (String vKey : v.getPropertyKeys()) {
                    //log.debug("Found vertex prop: " + vKey + " = " + v.getProperty(vKey));
                }
                if (TransactionalValue.class.equals(type)) {
                    Long tId = v.getProperty(TRANSACTION_ID);
                    T value = v.getProperty(TRIDENT_VALUE);
                    if (count <= 1) {
                        log.debug("Found transactional value: " + value.toString() + " of type: " + value.getClass().getName());
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
                    //log.debug("First load for object: "+index);
                    //objectMap.put(index, object);
                }
            }
            if (count == 0) {
                //log.debug("Found no object to return for: " + index + "!");
                returns.add(null);
            } else {
                if (foundObjects.size() > 1) {
                    log.debug("BlueprintsState found " + foundObjects.size() + " blueprints objects" + " index = " + index);
                }
                returns.addAll(foundObjects);
                objectMap.put(index, foundObjects);
            }



        } //end for keyList in keys


        long time = System.currentTimeMillis() - start;
        log.debug("Returning " + returns.size() + " objects for multiget in " + time + " ms");
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
        long start = System.currentTimeMillis();
        log.debug("Multiput called in Blueprints State: " + vals.size() + " values: " + keys.size() + " keys");
        for (int i = 0; i < keys.size(); i++) {
            T value = vals.get(i);
            List<Object> keyList = keys.get(i);
            GraphQuery query = graph.query();
            List<GroupByField> indices = new ArrayList<GroupByField>();
            for (int j = 0; j < keyList.size(); j++) {
                Object keyObject = keyList.get(j);
                if (keyObject instanceof GroupByField) {
                    GroupByField key = (GroupByField) keyObject;
                    indices.add(key);
                    query.has(key.getKey(), key.getValue());
                } else {
                    log.debug("Unknown key type: " + keyObject.getClass().getName() + " value: " + keyObject.toString());
                }
            }

            Iterable<Vertex> vertices = query.vertices();
            boolean objectExists = false;
            for (Vertex vertex : vertices) {
                objectExists = true;
                updateVertex(vertex, value);
            }
            if (!objectExists) {
                Vertex vertex = graph.addVertex(null);
                for (int z = 0; z < indices.size(); z++) {
                    vertex.setProperty(indices.get(z).getKey(), indices.get(z).getValue());
                }
                updateVertex(vertex, value);
            }
        }

        long time = System.currentTimeMillis() - start;
        log.debug("multiput finished in " + time + " ms");
    }

    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        log.debug("multiUpdate called Have " + keys.size() + " keys and " + updaters.size() + " updaters");
        List<T> updatedValues = new ArrayList<T>();
        Map<String, List<T>> objectMap = new HashMap<String, List<T>>();
        for (int i = 0; i < keys.size(); i++) {
            List<Object> keyList = keys.get(i);
            ValueUpdater<T> updater = updaters.get(i);
            //log.trace("Updater class: " + updater.getClass().getName());
            int count = 0;
            GraphQuery query = graph.query();
            String index = ""; //index holds a string of all query terms so we can return it instead
            for (Object keyObject : keyList) {
                if (keyObject instanceof GroupByField) {
                    GroupByField key = (GroupByField) keyObject;
                    query.has(key.getKey(), key.getValue());
                    index += key.toString();
                } else {
                    log.debug("Do not know how to create query from object type: " + keyObject.getClass().getName() + " value: " + keyObject.toString());
                }
            }
            //make sure we have some query parameters, otherwise blueprints returns all objects
            if (index.length() == 0) {
               // log.debug("Did not find any index terms to search on. Returning null");
                T updatedValue = updater.update(null);
                updatedValues.add(updatedValue);
                continue;
            }
            //log.trace("Index: " + index);
            //if already loaded object from blueprints, return that instead of querying
            if (objectMap.get(index) != null) {
                //log.debug("Already loaded object for "+index+", skipping db retrieval");
                updatedValues.addAll(objectMap.get(index));
                continue;
            }
            Iterable<Vertex> vertices = query.vertices();
            List<T> foundObjects = new ArrayList<T>();
            for (Vertex v : vertices) {
                count++;
                for (String vKey : v.getPropertyKeys()) {
                    //log.debug("Found vertex prop: " + vKey + " = " + v.getProperty(vKey));
                }
                if (TransactionalValue.class.equals(type)) {
                    Long tId = v.getProperty(TRANSACTION_ID);
                    T value = v.getProperty(TRIDENT_VALUE);
                    if (count <= 1) {
                        log.debug("Found transactional value: " + value.toString() + " of type: " + value.getClass().getName());
                    }
                    T transValue = (T) new TransactionalValue<T>(tId, value);
                    T updatedValue = (T) updater.update(transValue);
                    foundObjects.add(updatedValue);
                } else if (OpaqueValue.class.equals(type)) {
                    Long tId = v.getProperty(TRANSACTION_ID);
                    T value = v.getProperty(TRIDENT_VALUE);
                    T opaqueValue = (T) new OpaqueValue<T>(tId, value);
                    foundObjects.add(opaqueValue);
                } else {
                    T object = (T) v.getProperty(TRIDENT_VALUE);
                    T updatedValue = updater.update(object);
                    //returns.add(object);
                    foundObjects.add(updatedValue);
                    //log.debug("First load for object: "+index);
                    //objectMap.put(index, object);
                }
            }
            if (count == 0) {
                //log.debug("Found no object to return for: " + index + "!");
                T updatedValue = updater.update(null);
                foundObjects.add(updatedValue);
                objectMap.put(index, foundObjects);
                updatedValues.add(updatedValue);
            } else {
                if (foundObjects.size() > 1) {
                    log.debug("BlueprintsState found " + foundObjects.size() + " updated blueprints objects" + " index = " + index);
                }
                updatedValues.addAll(foundObjects);
                objectMap.put(index, foundObjects);
            }

            List<List<Object>> writeKeys = new ArrayList<List<Object>>();
            for (T updatedValue : updatedValues) {
                writeKeys.add(keyList);
            }
            multiPut(writeKeys, updatedValues);

        } //end for keyList in keys
//        if (log.isTraceEnabled()) {
//            for (T value : updatedValues) {
//                if (value != null) {
//                    log.trace("multiUpdate return: Updated value: " + value.toString());
//                } else {
//                    log.trace("multiUpdate return: Updated value: null");
//                }
//            }
//        }
        return updatedValues;

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
        log.debug("Begin commit called on: " + l);
    }

    public void commit(Long l) {
        log.debug("Commit called on " + l);
    }

    public List<Vertex> getVertices(String field, String identifier) {
        List<Vertex> vertices = new ArrayList<Vertex>();
        GraphQuery query = this.graph.query();
        query.has(field, identifier);
        Iterable<Vertex> iterable = query.vertices();
        for (Vertex v : iterable) {
            vertices.add(v);
        }
        return vertices;
    }

    public Vertex createObject(Map<String, Object> event) {
        Vertex v = this.graph.addVertex(null);
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
