/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import backtype.storm.tuple.Values;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class ObjectIdentifier extends BaseFunction{
    
    public void execute(TridentTuple tuple, TridentCollector collector){
        Map<String,Object> event = (Map<String,Object>)tuple.get(0);
        //TODO implement JEXL
        String identifier = event.get("TRACKID").toString();
        event.put("OBJECT_IDENTIFIER", identifier);
        System.out.println("ObjectIdentifier called.  Identifier: "+identifier);
        collector.emit(new Values(event, identifier));
        
        
    }
}
