/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import com.tinkerpop.blueprints.Vertex;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;


/**
 *
 * @author jwalton
 */
public class BlueprintsQuery extends BaseQueryFunction<BlueprintsState, Map<String,Object>>{

    public List batchRetrieve(BlueprintsState s, List list) {
        System.out.println("batchRetrieve called on : "+list.toString());
        
        return null;
    }

    public void execute(TridentTuple tt, Map<String,Object> event, TridentCollector tc) {
        System.out.println("Query execute called on: "+tt.toString());
    }
    
}
