/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import be.datablend.blueprints.impls.mongodb.MongoDBGraph;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 *
 * @author jwalton
 */
public class SerializableMongoDBGraph implements Serializable {
    
    private String host;
    private int port;
    private String user;
    private String password;
    private transient Graph graph;

    public SerializableMongoDBGraph(String host, int port) {
        this.host = host;
        this.port = port;
        graph = new MongoDBGraph(host, port);
//        new TinkerGraph();
    }

    public SerializableMongoDBGraph(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        graph = new MongoDBGraph(host, port, user, password);
    }

    private void readObject(ObjectInputStream ois) {
        try {
            ois.defaultReadObject();
            if (user == null) {
                //System.out.println("Creating graph with just host and port");
                graph = new MongoDBGraph(host, port);
            } else {
                //System.out.println("Creating graph with host, port, user, and password");
                graph = new MongoDBGraph(host, port, user, password);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public Graph getGraph(){
        return this.graph;
    }
}
