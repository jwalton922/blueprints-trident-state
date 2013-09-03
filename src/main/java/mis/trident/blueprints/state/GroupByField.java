/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.trident.blueprints.state;

import java.io.Serializable;

/**
 *
 * @author jwalton
 */
public class GroupByField implements Serializable{
    
    private String key;
    private Object value;
    
    public GroupByField(String key, Object value){
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
    
    @Override
    public String toString(){
        return "\""+key+"="+this.value.toString()+"\"";
    }
    
    @Override
    public boolean equals(Object o){
        boolean isEqual = false;
        if(o instanceof GroupByField){
            GroupByField test = (GroupByField)o;
            if(this.key.equalsIgnoreCase(test.getKey()) && this.value.equals(test.getValue())){
                isEqual = true;
            }
        }
        
        return isEqual;
    }
}
