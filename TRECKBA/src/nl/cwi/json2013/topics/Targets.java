
package nl.cwi.json2013.topics;

import javax.annotation.Generated;

import com.fasterxml.jackson.annotation.JsonInclude;


/**
 * Array of target objects describing entities within reference knowledge-base-like systems, such as en.wikipedia.org, freebase.com, twitter.com, etc.
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
public class Targets {

   
    
    public String entity_type;
    public String group;
    public String target_id;
    
    
   

}
