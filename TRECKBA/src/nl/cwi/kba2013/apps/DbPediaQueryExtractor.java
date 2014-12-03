package nl.cwi.kba2013.apps;
//import arq.query;
//
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;







public class DbPediaQueryExtractor {
	public static void main(String [] args){
		//SparqlUtils spa =new SparqlUtils();
		//String entity = SparqlUtils.escape("William_H._Miller_(writer)");
		//String queryString ="PREFIX dbres: <http://dbpedia.org/resource/> SELECT * WHERE {<http://dbpedia.org/resource/"+ entity+ "<http://www.w3.org/2000/01/rdf-schema#label> ?o FILTER (langMatches(lang(?o),\"en\"))}";
		
		
//		final ParameterizedSparqlString queryString = new ParameterizedSparqlString(
//                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
//                "SELECT * WHERE\n" +
//                "{\n" +
//                "  ?entity rdfs:label ?o\n" +
//                "  FILTER (langMatches(lang(?o),\"en\"))\n" +
//                "}\n"
//			
//                ) 
//        
//        {{
//            setNsPrefix( "dbres", "http://dbpedia.org/resource/" );
//        }};
//		

		final ParameterizedSparqlString queryString = new ParameterizedSparqlString(
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "SELECT distinct * WHERE\n" +
                "{\n"+
                "{\n"+
              "SELECT distinct ?o WHERE " +
              "{" +
              "?entity <http://xmlns.com/foaf/0.1/name>  ?o ." +
              "}" +
              "}" +
              "UNION\n"+
              "{\n" +
              "SELECT distinct ?o WHERE"+
              "{\n"+
              "  ?entity rdfs:label ?o\n" +
              "}"+
              "}"+
             
             "UNION\n"+
             "{\n" +
             "SELECT distinct ?o WHERE"+
             "{\n"+
             "  ?x rdfs:label ?o ." +
             " ?x <http://dbpedia.org/ontology/wikiPageRedirects> ?entity \n" +
            "}"+
            "}"+
           
            "}"
                ) 
        
        {{
            setNsPrefix( "dbres", "http://dbpedia.org/resource/" );
        }};


//        "SELECT distinct ?o WHERE " +
//        "{" +
//        "?entity <http://xmlns.com/foaf/0.1/name>  ?o ." +
//        "}" +
//        "}" +
//        "UNION\n"+
//        "{\n" +
//        "SELECT distinct ?o WHERE"+
//        "{\n"+
//        "  ?entity rdfs:label ?o\n" +
//        "}"+
//        "}"+
//       
//       "UNION\n"+
//       "{\n" +
//       "SELECT distinct ?o WHERE"+
//       "{\n"+
//       "  ?x rdfs:label ?o ." +
//       " ?x <http://dbpedia.org/ontology/wikiPageRedirects> ?entity \n" +
//      "}"+
//      "}"+
//                      
//		"UNION\n"+
//		"{\n" +
//	
        
//   	 		"SELECT distinct ?o WHERE"+
//   	 		"{\n"+
//   	 		"  ?x rdfs:label ?o ." +
//   	 		" ?x <http://dbpedia.org/ontology/wikiPageRedirects> ?ent \n" +
//   	 		"}"+
//   			 "}"+
//   	

		
        // Entity is the same. 
        //String str ="/export/scratch2/TREC-kba/TREC-KBA-2013/data/kbaqueries/trec-kba-topics-wiki-names-missing.txt";
        //String str ="/export/scratch2/TREC-kba/TREC-KBA-2013/data/kbaqueries/name-variants/trec-kba-topics-wiki-names-missing.txt";
        //String str ="/export/scratch2/TREC-kba/TREC-KBA-2013/data/kbaqueries/trec-kba-topics-wiki-names.txt";
        String str ="/export/scratch2/ranking/cano_names.txt";
 
        
        
      // String entity = "William_H._Miller_(writer)";

        // Now retrieve the URI for dbres, concatentate it with entity, and use
        // it as the value of ?entity in the query.
//        queryString.setIri( "?entity", queryString.getNsPrefixURI( "dbres" )+entity );
        HashSet <String> hs2 = new HashSet <String>();
//        hs2 = openFile(str);
        hs2 = openFile(str);
        File file = new File("/export/scratch2/TREC-kba/TREC-KBA-2013/data/kbaqueries/name-variants/trec-kba-topics-wiki-names-with-dbpedia-name-label-redirect.txt");
        PrintWriter writer = null;
        try {
			 writer = new PrintWriter(file, "UTF-8");
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        // Show the query.
      java.util.Iterator<String> it = hs2.iterator();
        while (it.hasNext()){
        	 try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        String entity = it.next();
        queryString.setIri( "?entity", queryString.getNsPrefixURI( "dbres" )+entity );
        //System.out.println(queryString.toString());
             
        		
		  QueryExecution qexec = getResult(queryString.toString());
		  
		  try {
				ResultSet results = qexec.execSelect();
			for (; results.hasNext();)
			{
				QuerySolution soln = results.nextSolution();
			
			System.out.print(entity+ "\t" + soln.get("?o").toString() + "\n");
			writer.println(entity+ "\t" + soln.get("?o").toString());
			
			}
			} finally {qexec.close();
			}
        }
        writer.close();
        System.out.println("Sucessfull !!!!!!!");
		}
	
	
	public static QueryExecution getResult(String queryString){
		Query query = QueryFactory.create(queryString);

	  QueryExecution qexec = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query);
	  	
		
	return qexec;
		 
	}
	
	public static HashSet <String> openFile(String file){
		HashSet <String> hs = new HashSet <String>();
		try {
    		File fileDir = new File(file);
     
    		BufferedReader in = new BufferedReader(
    		   new InputStreamReader(
                          new FileInputStream(fileDir), "UTF8"));
     
    	
    		String str;
    		while (( str = in.readLine()) != null) {
    		   // System.out.println(str.trim());
    		    
    			hs.add(str.trim());
    		}
     
                    in.close();
    	    } 
    	    catch (UnsupportedEncodingException e) 
    	    {
    			System.out.println(e.getMessage());
    	    } 
    	    catch (IOException e) 
    	    {
    			System.out.println(e.getMessage());
    	    }
    	    catch (Exception e)
    	    {
    			System.out.println(e.getMessage());
    	    }
		return hs;
	}
}

