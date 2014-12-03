package nl.cwi.kba2013.apps;
import com.hp.hpl.jena.query.ParameterizedSparqlString;

public class PssExample {
    public static void main( String[] args ) {
        // Create a parameterized SPARQL string for the particular query, and add the 
        // dbres prefix to it, for later use.
        final ParameterizedSparqlString queryString = new ParameterizedSparqlString(
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "SELECT * WHERE\n" +
                "{\n" +
                "  ?entity rdfs:label ?o\n" +
                "  FILTER (langMatches(lang(?o),\"en\"))\n" +
                "}\n"
                ) 
        
        {{
            setNsPrefix( "dbres", "http://dbpedia.org/resource/" );
        }};

        // Entity is the same. 
        final String entity = "William_H._Miller_(writer)";

        // Now retrieve the URI for dbres, concatentate it with entity, and use
        // it as the value of ?entity in the query.
        queryString.setIri( "?entity", queryString.getNsPrefixURI( "dbres" )+entity );

        // Show the query.
        System.out.println( queryString.toString() );
    }
}
