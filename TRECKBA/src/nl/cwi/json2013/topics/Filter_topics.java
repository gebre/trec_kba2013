
package nl.cwi.json2013.topics;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Generated;



import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Describes the topic entities for a KBA filtering task.
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "$schema",
    "topic_set_id",
    "targets"
})
public class Filter_topics {

    /**
     * URI of this JSON schema document.
     * 
     */
	 public static class Factory {

		    private static final ObjectMapper mapper = new ObjectMapper();

		    public Factory() {

		    }

		    public Filter_topics loadTopics(String inputfilelocation) {
		      Filter_topics ft = null;
		      try {
		        ft = mapper.readValue(new File(inputfilelocation), Filter_topics.class);
		      } catch (JsonParseException e) {
		        e.printStackTrace();
		      } catch (JsonMappingException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      }

		      return ft;
		    }

		    public Filter_topics loadTopics(BufferedReader br) {
		      Filter_topics ft = null;
		      try {
		        ft = mapper.readValue(br, Filter_topics.class);
		      } catch (JsonParseException e) {
		        e.printStackTrace();
		      } catch (JsonMappingException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      }

		      return ft;
		    }

		    public Filter_topics loadTopics(InputStream is) {
		      Filter_topics ft = null;
		      try {
		        ft = mapper.readValue(is, Filter_topics.class);
		      } catch (JsonParseException e) {
		        e.printStackTrace();
		      } catch (JsonMappingException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      }

		      return ft;
		    }
		  }
	
	 @JsonProperty("$schema")
	    private static Filter_topics.$schema $schema = Filter_topics.$schema.fromValue("http://trec-kba.org/schemas/v1.1/filter-topics.json");
	 @JsonProperty("$schema")
	    public Filter_topics.$schema get$schema() {
	        return $schema;
	    }

	    /**
	     * URI of this JSON schema document.
	     * 
	     */
	    @JsonProperty("$schema")
	    public void set$schema(Filter_topics.$schema $schema) {
	        this.$schema = $schema;
	    }
    /**
     * Unique string that identifies this set of topic entities
     * (Required)
     * 
     */
    @JsonProperty("topic_set_id")
    private String topic_set_id;
    /**
     * Array of target objects describing entities within reference knowledge-base-like systems, such as en.wikipedia.org, freebase.com, twitter.com, etc.
     * (Required)
     * 
     */
    @JsonProperty("targets")
   // private Targets targets;
    public Targets[] targets;
  

    /**
     * Unique string that identifies this set of topic entities
     * (Required)
     * 
     */
    @JsonProperty("topic_set_id")
    public String getTopic_set_id() {
        return topic_set_id;
    }

    
    
    @JsonProperty("topic_set_id")
    public void setTopic_set_id(String topic_set_id) {
        this.topic_set_id = topic_set_id;
    }

    /**
     * Array of target objects describing entities within reference knowledge-base-like systems, such as en.wikipedia.org, freebase.com, twitter.com, etc.
     * (Required)
     * 
     */
    @JsonProperty("targets")
    public Targets [] getTargets() {
        return Arrays.copyOf(targets, targets.length);
    }

    /**
     * Array of target objects describing entities within reference knowledge-base-like systems, such as en.wikipedia.org, freebase.com, twitter.com, etc.
     * (Required)
     * 
     */
    @SuppressWarnings("unchecked")
	@JsonProperty("targets")
    public void setTargets(Targets [] targets) {
        this.targets =  Arrays.copyOf(targets, targets.length);
    }


    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object other) {
        return EqualsBuilder.reflectionEquals(this, other);
    }
   
    @Generated("com.googlecode.jsonschema2pojo")
    public static enum $schema {

        HTTP_TREC_KBA_ORG_SCHEMAS_V_1_1_FILTER_TOPICS_JSON("http://trec-kba.org/schemas/v1.1/filter-topics.json");
        private final String value;
        private static Map<String, Filter_topics.$schema> constants = new HashMap<String, Filter_topics.$schema>();

        static {
            for (Filter_topics.$schema c: Filter_topics.$schema.values()) {
                constants.put(c.value, c);
            }
        }

        private $schema(String value) {
            this.value = value;
        }

        @JsonValue
        @Override
        public String toString() {
            return this.value;
        }

        @JsonCreator
        public static Filter_topics.$schema fromValue(String value) {
            Filter_topics.$schema constant = constants.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }
    


}
