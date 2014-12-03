
package nl.cwi.json2013.run;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import nl.cwi.json2012.run.Filter_run;
import nl.cwi.json2012.run.Filter_run.Run_type;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;


/**
 * Describes a run of a KBA system and is first line of a text file containing one result per line.  See http://trec-kba.org/trec-kba-2013.shtml#submissions
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "$schema",
    "task_id",
    "topic_set_id",
    "corpus_id",
    "team_id",
    "team_name",
    "poc_name",
    "poc_email",
    "system_id",
    "run_type",
    "system_description",
    "system_description_short"
})
public class Run_info {
	 public static class Factory {

		    private static final ObjectMapper mapper = new ObjectMapper();

		    public Factory() {

		    }

		    /**
		     * Creates a new Run_info object, with most default values already initialized.
		     * 
		     * @return
		     */
		    public Run_info create() {

		      Run_info fr = new Run_info();
		      fr.set$schema(Run_info.$schema);
		      fr.setTask_id("kba-streamcorpus-2013-v0_2_0");
		      fr.setRun_type(Run_type.AUTOMATIC);

		      return fr;
		    }

		    /**
		     * Creates a new Run_info object, with most default values already initialized.
		     * 
		     * @param team The team name
		     * @param system The run ID
		     * @param desc A description
		     * @return
		     */
		    public Run_info create(String team, String system, String desc, String desc_s,
		        String corpus_id) {

		      Run_info fr = create();

		      fr.setTeam_id(team);
		      fr.setSystem_id(system);
		      fr.setSystem_description(desc);
		      fr.setSystem_description_short(desc_s);
		      fr.setCorpus_id(corpus_id);
		      fr.setPoc_email("gebre@cwi.nl");
		      fr.setPoc_name("The Information Acess of CWI ");
		      fr.setTeam_name("CWI");

		      return fr;
		    }

		    public void serializeRun(Run_info fr, File f) {
		      try {
		        mapper.writeValue(f, fr);
		      } catch (JsonGenerationException e) {
		        e.printStackTrace();
		      } catch (JsonMappingException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      }
		    }

		    public String toPrettyJSON(Run_info fr) {
		      try {
		        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(fr);
		      } catch (JsonGenerationException e) {
		        e.printStackTrace();
		      } catch (JsonMappingException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      }
		      return null;
		    }

		    public String toJSON(Run_info fr) {
		      try {
		        return mapper.writeValueAsString(fr);
		      } catch (JsonGenerationException e) {
		        e.printStackTrace();
		      } catch (JsonMappingException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      }
		      return null;
		    }

		    public Run_info loadRun(String inputfilelocation) {
		      Run_info fr = null;
		      try {    	   
		        fr = mapper.readValue(new File(inputfilelocation), Run_info.class);
		      } catch (JsonParseException e) {
		        e.printStackTrace();
		      } catch (JsonMappingException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      }

		      return fr;
		    }
		   
		  public Run_info loadRunFromString(String run) {
		      Run_info fr = null;
		      try {    	   
		    	  
		        fr = mapper.readValue(run, Run_info.class);
		      } catch (JsonParseException e) {
		        e.printStackTrace();
		      } catch (JsonMappingException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      }

		      return fr;
		    }
		  }


    /**
     * URI of this JSON schema document.
     * 
     */
    @JsonProperty("$schema")
    private static Run_info.$schema $schema = Run_info.$schema.fromValue("http://trec-kba.org/schemas/v1.1/filter-run.json");
    /**
     * Unique string that identifies the task your system is performing
     * (Required)
     * 
     */
    @JsonProperty("task_id")
    private String task_id;
    /**
     * Unique string that identifies the topics for which your system is filtering
     * (Required)
     * 
     */
    @JsonProperty("topic_set_id")
    private String topic_set_id;
    /**
     * Unique string that identifies the corpus on which your system operated
     * (Required)
     * 
     */
    @JsonProperty("corpus_id")
    private String corpus_id;
    /**
     * Unique string that your team selected for identifying itself to TREC
     * (Required)
     * 
     */
    @JsonProperty("team_id")
    private String team_id;
    /**
     * Display name of team.
     * (Required)
     * 
     */
    @JsonProperty("team_name")
    private String team_name;
    /**
     * Name of the primary point of contact for this run submission.
     * (Required)
     * 
     */
    @JsonProperty("poc_name")
    private String poc_name;
    /**
     * Email address of the primary point of contact for this run submission.
     * (Required)
     * 
     */
    @JsonProperty("poc_email")
    private String poc_email;
    /**
     * Unique string that you select for distinguishing this system-&-configuration from other runs you submit
     * (Required)
     * 
     */
    @JsonProperty("system_id")
    private String system_id;
    /**
     * Flags for categorizing runs.
     * (Required)
     * 
     */
    @JsonProperty("run_type")
    private Run_info.Run_type run_type;
    /**
     * Human readable description of this system.  Please be verbose, e.g. 600 to 1000 characters.
     * (Required)
     * 
     */
    @JsonProperty("system_description")
    private String system_description;
    /**
     * Short human readable description of this system.  140 characters or less.
     * (Required)
     * 
     */
    @JsonProperty("system_description_short")
    private String system_description_short;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * URI of this JSON schema document.
     * 
     */
    @JsonProperty("$schema")
    public Run_info.$schema get$schema() {
        return $schema;
    }

    /**
     * URI of this JSON schema document.
     * 
     */
    @JsonProperty("$schema")
    public void set$schema(Run_info.$schema $schema) {
        this.$schema = $schema;
    }

    /**
     * Unique string that identifies the task your system is performing
     * (Required)
     * 
     */
    @JsonProperty("task_id")
    public String getTask_id() {
        return task_id;
    }

    /**
     * Unique string that identifies the task your system is performing
     * (Required)
     * 
     */
    @JsonProperty("task_id")
    public void setTask_id(String task_id) {
        this.task_id = task_id;
    }

    /**
     * Unique string that identifies the topics for which your system is filtering
     * (Required)
     * 
     */
    @JsonProperty("topic_set_id")
    public String getTopic_set_id() {
        return topic_set_id;
    }

    /**
     * Unique string that identifies the topics for which your system is filtering
     * (Required)
     * 
     */
    @JsonProperty("topic_set_id")
    public void setTopic_set_id(String topic_set_id) {
        this.topic_set_id = topic_set_id;
    }

    /**
     * Unique string that identifies the corpus on which your system operated
     * (Required)
     * 
     */
    @JsonProperty("corpus_id")
    public String getCorpus_id() {
        return corpus_id;
    }

    /**
     * Unique string that identifies the corpus on which your system operated
     * (Required)
     * 
     */
    @JsonProperty("corpus_id")
    public void setCorpus_id(String corpus_id) {
        this.corpus_id = corpus_id;
    }

    /**
     * Unique string that your team selected for identifying itself to TREC
     * (Required)
     * 
     */
    @JsonProperty("team_id")
    public String getTeam_id() {
        return team_id;
    }

    /**
     * Unique string that your team selected for identifying itself to TREC
     * (Required)
     * 
     */
    @JsonProperty("team_id")
    public void setTeam_id(String team_id) {
        this.team_id = team_id;
    }

    /**
     * Display name of team.
     * (Required)
     * 
     */
    @JsonProperty("team_name")
    public String getTeam_name() {
        return team_name;
    }

    /**
     * Display name of team.
     * (Required)
     * 
     */
    @JsonProperty("team_name")
    public void setTeam_name(String team_name) {
        this.team_name = team_name;
    }

    /**
     * Name of the primary point of contact for this run submission.
     * (Required)
     * 
     */
    @JsonProperty("poc_name")
    public String getPoc_name() {
        return poc_name;
    }

    /**
     * Name of the primary point of contact for this run submission.
     * (Required)
     * 
     */
    @JsonProperty("poc_name")
    public void setPoc_name(String poc_name) {
        this.poc_name = poc_name;
    }

    /**
     * Email address of the primary point of contact for this run submission.
     * (Required)
     * 
     */
    @JsonProperty("poc_email")
    public String getPoc_email() {
        return poc_email;
    }

    /**
     * Email address of the primary point of contact for this run submission.
     * (Required)
     * 
     */
    @JsonProperty("poc_email")
    public void setPoc_email(String poc_email) {
        this.poc_email = poc_email;
    }

    /**
     * Unique string that you select for distinguishing this system-&-configuration from other runs you submit
     * (Required)
     * 
     */
    @JsonProperty("system_id")
    public String getSystem_id() {
        return system_id;
    }

    /**
     * Unique string that you select for distinguishing this system-&-configuration from other runs you submit
     * (Required)
     * 
     */
    @JsonProperty("system_id")
    public void setSystem_id(String system_id) {
        this.system_id = system_id;
    }

    /**
     * Flags for categorizing runs.
     * (Required)
     * 
     */
    @JsonProperty("run_type")
    public Run_info.Run_type getRun_type() {
        return run_type;
    }

    /**
     * Flags for categorizing runs.
     * (Required)
     * 
     */
    @JsonProperty("run_type")
    public void setRun_type(Run_info.Run_type run_type) {
        this.run_type = run_type;
    }

    /**
     * Human readable description of this system.  Please be verbose, e.g. 600 to 1000 characters.
     * (Required)
     * 
     */
    @JsonProperty("system_description")
    public String getSystem_description() {
        return system_description;
    }

    /**
     * Human readable description of this system.  Please be verbose, e.g. 600 to 1000 characters.
     * (Required)
     * 
     */
    @JsonProperty("system_description")
    public void setSystem_description(String system_description) {
        this.system_description = system_description;
    }

    /**
     * Short human readable description of this system.  140 characters or less.
     * (Required)
     * 
     */
    @JsonProperty("system_description_short")
    public String getSystem_description_short() {
        return system_description_short;
    }

    /**
     * Short human readable description of this system.  140 characters or less.
     * (Required)
     * 
     */
    @JsonProperty("system_description_short")
    public void setSystem_description_short(String system_description_short) {
        this.system_description_short = system_description_short;
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

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Generated("com.googlecode.jsonschema2pojo")
    public static enum $schema {

        HTTP_TREC_KBA_ORG_SCHEMAS_V_1_1_FILTER_RUN_JSON("http://trec-kba.org/schemas/v1.1/filter-run.json");
        private final String value;
        private static Map<String, Run_info.$schema> constants = new HashMap<String, Run_info.$schema>();

        static {
            for (Run_info.$schema c: Run_info.$schema.values()) {
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
        public static Run_info.$schema fromValue(String value) {
            Run_info.$schema constant = constants.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    @Generated("com.googlecode.jsonschema2pojo")
    public static enum Run_type {

        MANUAL("manual"),
        AUTOMATIC("automatic"),
        OTHER("other");
        private final String value;
        private static Map<String, Run_info.Run_type> constants = new HashMap<String, Run_info.Run_type>();

        static {
            for (Run_info.Run_type c: Run_info.Run_type.values()) {
                constants.put(c.value, c);
            }
        }

        private Run_type(String value) {
            this.value = value;
        }

        @JsonValue
        @Override
        public String toString() {
            return this.value;
        }

        @JsonCreator
        public static Run_info.Run_type fromValue(String value) {
            Run_info.Run_type constant = constants.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
