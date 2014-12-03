/*******************************************************************************
 * Copyright 2013 Gebre
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package nl.cwi.kba2013.apps;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import nl.cwi.helpers.NGramExtractor;
//import nl.cwi.json2012.run.Filter_run;
import nl.cwi.json2013.run.Run_info;
import nl.cwi.json2013.topics.Filter_topics;
import nl.cwi.json2013.topics.Targets;
import nl.cwi.kba2013.thrift.ContentItem;
//import nl.cwi.kba2013.apps.KbaDocExtractor.MyMapper;
import nl.cwi.kba2013.thrift.Target;
import nl.cwi.kba2013.thrift.bin.StreamItemWritable;
import nl.cwi.kba2013.thrift.bin.ThriftFileInputFormat;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat; 

/**
 * TThis program extracts features from either the training set or test set.
 * documents based on mere lexical matching.
 * 
 * @author Gebremeskel
 * 
 */
public class KbaNameVariantMatchFeatureExtractor extends Configured implements Tool {

	public static final String QUERYFILEPATH_HDFS = "kba.topicfilelocation";
	public static final String WIKI_HDFS = "wiki";
	public static final String TrainTest_HDFS = "tt";
	public static final String CONTEXT_HDFS = "context";
	public static final String LABELSFILEPATH_HDFS = "kba.labels";
	public static final String ANNOFILEPATH_HDFS = "kba.anno";
	public static final String PPR_HDFS = "ppr";
	public static final String MYVER = "myver";
	public static final String GCLD_HDFS = "gcld";
	public static Path anno_file;
	public static Map<String, String> all_attr = new LinkedHashMap<String, String>();
	public static Map<String, HashSet<String>> DbPedia = new LinkedHashMap<String, HashSet<String>>();
	public static Map<String, Integer> Annotation = new LinkedHashMap<String, Integer>();
	public static Map<String, HashMap<String, Double>> Ppr = new LinkedHashMap<String, HashMap<String, Double>>();
	public static Map<String, HashMap<String, Double>> Gcld = new LinkedHashMap<String, HashMap<String, Double>>();
	public static Map<String, String> MyVer = new HashMap<String, String>();
	public static Map<String, String> ContextL = new LinkedHashMap<String, String>();
	public static Map<String, String> ContextR = new LinkedHashMap<String, String>();
	public static Map<String, HashMap<String, Integer>> ContextFreqsL = new LinkedHashMap<String, HashMap<String, Integer>>();
	public static Map<String, HashMap<String, Integer>> ContextFreqsR = new LinkedHashMap<String, HashMap<String, Integer>>();
	
	public static Map<String, String> WikiContent = new LinkedHashMap<String, String>();
	public static Map<String,  String> TrainTest = new LinkedHashMap<String, String>();
	// public static Map<String, String> Attr ;

	public static final String RUNTAG = "kba.runtag";
	public static final String TEAMNAME = "kba.teamname";

	protected enum Counter {
		documents, doc_with_body, body_raw, body_with_clean_html, body_with_clean_vissible
	};

	private static final Logger LOG = Logger
			.getLogger(KbaNameVariantMatchFeatureExtractor.class);

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		private static final Text out = new Text();
		private static final Text none = new Text("");

		private String runtag;
		private String teamname;

		@Override
		public void reduce(Text date, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			context.write(out, none);

		}
	}

	/**
	 * Emits date, PairOfStringLong pairs, where the string contains the docno
	 * and the topic and the long contains the score.
	 * 
	 * 
	 */
	public static class MyMapper extends
			Mapper<Text, StreamItemWritable, Text, Text> {

		private static final Text stream_doc_id = new Text();
		public static final int scale = 1000;
		// private static final Text featurLine = new Text();
		private Map<String, Pattern> topicregexes = new LinkedHashMap<String, Pattern>();
		private Map<String, HashSet<String>> partialtopics = new LinkedHashMap<String, HashSet<String>>();
		private Filter_topics ft = null;

		/**
		 * Used to load the queries.
		 */
		@Override
		public void setup(Context context) throws IOException,
		InterruptedException {
	super.setup(context);

	// String ppr_f = new
	// Path(context.getConfiguration().get(PPR_HDFS)).toString();
	anno_file = new Path(context.getConfiguration().get(
			ANNOFILEPATH_HDFS));

	DbPedia = loadLabels(LABELSFILEPATH_HDFS);
//	for (String kk : DbPedia.keySet()) {
//		System.out.println(kk + "\t" + DbPedia.get(kk));
//	}
//	System.out.println("The size of DbPedia" + DbPedia.size());
	Annotation = loadAnnotation(ANNOFILEPATH_HDFS);
	// MyVer = loadMyVer(MYVER);

	Ppr = loadPPR(PPR_HDFS);
	Gcld = loadGCLD(GCLD_HDFS);
	WikiContent = loadWiki(WIKI_HDFS);
	TrainTest = loadTrainTest(TrainTest_HDFS);
	//System.out.println("Wiki Content:" + WikiContent.size());
	loadContext(CONTEXT_HDFS);
	extractNgrmFrequency();
	/*
	 * System.out.println("What are the strings:\n"); for(String k:
	 * DbPedia.keySet()) System.out.println(k + "\t"+
	 * DbPedia.get(k).size() );
	 */
	//System.out.println("Annotations size :"
		//	+ Annotation.keySet().size());
	//System.out.println("DbPEdia Keys :" + DbPedia.keySet().size());

	loadTopics(QUERYFILEPATH_HDFS);
	

}
		/**
		 * Loads the queries from the JSON topic file.
		 * 
		 * @param context
		 */

		/**
		 * Not used
		 */
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		public void map(Text key, StreamItemWritable value, Context context)
				throws IOException, InterruptedException {

			
			
			context.getCounter(Counter.documents).increment(1);

			all_attr.put("gcld", "0");
			all_attr.put("jac", "0");
			all_attr.put("cos", "0");
			all_attr.put("kl", "0");
			all_attr.put("ppr", "0");
			all_attr.put("s_form", "0");
			all_attr.put("contxR", "0");
			all_attr.put("contxL", "0");
			all_attr.put("FirstPos", "0");
			all_attr.put("LastPos", "0");
			all_attr.put("LengthBody", "0");
			all_attr.put("FirstPosNorm", "0");
			all_attr.put("MentionsBody", "0");
			all_attr.put("RelatedBody", "0");
			all_attr.put("Spread", "0");
			all_attr.put("LastPosNorm", "0");
			all_attr.put("SpreadNorm", "0");
			all_attr.put("LengthAnchor", "0");
			all_attr.put("Source", "0");
			all_attr.put("LengthTitle", "0");
			all_attr.put("partial", "0");
			all_attr.put("MentionsAnchor", "0");
			all_attr.put("Relatedtitle", "0");
			all_attr.put("English", "0");
			all_attr.put("RelatedAnchor", "0");
			all_attr.put("MentionsTitle", "0");
			all_attr.put("Class", "0");

			
					
			if(value.getBody()!=null){
			
				String body;
			
				ContentItem b = value.getBody();
				if (value.getBody().getRaw().length!= 0){
					byte[] r = b.getRaw();
					
					String encoding;
					if (b.getEncoding()!=null){
						encoding=b.getEncoding();
					}
					else{
						encoding="UTF-8";
					}
					
					 body = new String(r, encoding);
					
					
//					if ((value.getBody().getClean_visible() != null))
//					{
//							body = b.getClean_visible().toString();
						
					String title = null;
					String anchor = null;
					if (value.getOther_content() != null) {
						if (value.getOther_content().get("title") != null) {
							if (value.getOther_content().get("title")
									.getClean_visible() != null) {
								title = new String(value.getOther_content()
										.get("title").getClean_visible());
							} else if (value.getOther_content().get("title")
									.getClean_html() != null) {
								title = new String(value.getOther_content()
										.get("title").getClean_html());
							} else if (value.getOther_content().get("title")
									.getRaw() != null) {
								title = new String(value.getOther_content()
										.get("title").getRaw());
							} else
								title = "";
						}
						if (value.getOther_content().get("anchor") != null) {
							if (value.getOther_content().get("anchor")
									.getClean_visible() != null) {
								anchor = new String(value.getOther_content()
										.get("anchor").getClean_visible());
							} else if (value.getOther_content().get("anchor")
									.getClean_html() != null) {
								anchor = new String(value.getOther_content()
										.get("anchor").getClean_html());
							} else if (value.getOther_content().get("anchor")
									.getRaw() != null) {
								anchor = new String(value.getOther_content()
										.get("anchor").getRaw());
							} else {
								anchor = "";
							}
						}
					}
					if (title == null)
						title = "";
					if (anchor == null)
						anchor = "";

					// lowercase all

					body = body.toLowerCase();
					title = title.toLowerCase();
					anchor = anchor.toLowerCase();
					String url = new String();
					String source = new String();

					if(value.getSource()!=null){
						source = value.getSource();	
					}
					
					
					String streamid = value.getStream_id();

					all_attr.put("English", "0");

					Integer lengthTitle = new StringTokenizer(title)
							.countTokens();
					Integer lengthBody = new StringTokenizer(body)
							.countTokens();
					Integer lengthAnchor = new StringTokenizer(anchor)
							.countTokens();

					all_attr.put("LengthTitle", lengthTitle.toString());
					all_attr.put("LengthBody", lengthBody.toString());
					all_attr.put("LengthAnchor", lengthAnchor.toString());

					String sourcevalue = new String(value.getSource());

					all_attr.put("Source", source.toString());

					all_attr.put("RelatedBody", entityMentionCounter(body));
					all_attr.put("Relatedtitle", entityMentionCounter(title));
					all_attr.put("RelatedAnchor", entityMentionCounter(anchor));

					String title_body_anchor = body + " " + title + " "
							+ anchor;
					title_body_anchor =title_body_anchor.replaceAll("\\s+"," ");
					//Log.info("title_body_anchor"+ title_body_anchor);
					for (String topic : topicregexes.keySet()) {
						// int relde=0;
						// long count = 0;
						String topic_head = "";
						if (topic.startsWith("https://twitter.com/")) {
							topic_head = topic.split("/")[3];
						} else {
							topic_head = topic.split("/")[4];
						}
						
						stream_doc_id.set(streamid + "_" + topic);
						int anno_value = 0;

						 if (Annotation.containsKey(stream_doc_id.toString()))
						{
						
						  if
						  (Annotation.get(stream_doc_id.toString()).intValue()
						  == 2) {
							  anno_value = 1; // relevant
						  all_attr.put( "Class", new Integer(1) .toString());
						  } else if (Annotation.get(stream_doc_id.toString())
						  .intValue() == 1) { anno_value = -3;
						  
						  }
						}
						
						 
						 
						HashSet<String> canonical = new HashSet<String>();
						if (!topic.startsWith("https://twitter.com/")){
							//Log.info("The topic at hand"+ topic);
							//canonical.add(topic.split("/")[4].replace("_", " "));
							canonical.add(topic.split("/")[4].replaceAll("_", " ").replace("(", "").replace(")", "").replaceAll("\\s+"," ").toLowerCase());
							//Log.info("hmmmmm" +canonical);
						}
						else{
							canonical.add(topic.split("/")[3].replaceAll("\\s+"," ").toLowerCase());
						}
						
						HashSet<String> canonical_partial = new HashSet<String>();
						if (!topic.startsWith("https://twitter.com/")){
							canonical_partial.addAll(listremove(Arrays.asList(topic.split("/")[4].replaceAll("_", " ").replace("(", "").replace(")", "").replaceAll("\\s+"," ").toLowerCase().split(" "))));
							//canonical_partial.addAll(Arrays.asList(topic.split("/")[4].replaceAll("_", " ").replace("(", "").replace(")", "").replaceAll("\\s+"," ").toLowerCase().split(" ")));
							//Log.info("what the hell" + canonical_partial.toString());
						}
						else {
							//canonical_partial.addAll(listremove(Arrays.asList(topic.split("/")[3].replaceAll("\\s+"," ").toLowerCase().split(" "))));
							canonical_partial.addAll(Arrays.asList(topic.split("/")[3].replaceAll("\\s+"," ").toLowerCase().split(" ")));
							//Log.info("what the hell" +Arrays.asList(topic.replaceAll(",",  " ").split("/")[3]));
						}

						HashSet<String> label_name_redirect_canonical_partial =new HashSet<String>();
						
						try{
						for(String all_str: DbPedia.get(topic)){
							
							//label_name_redirect_canonical_partial.addAll(listremove(Arrays.asList(all_str.replaceAll(",", " ").split(" ")))); 
							label_name_redirect_canonical_partial.addAll(Arrays.asList(all_str.replaceAll(",", " ").split(" ")));
							
						}
						} catch (NullPointerException e) {
							//Log.info("what what " +topic+ "\t" );
						} 
							
						
						HashSet<String> label_name_redirect_canonical =new HashSet<String>();
						label_name_redirect_canonical =DbPedia.get(topic);
						
						if (containsMention(title_body_anchor,
								label_name_redirect_canonical_partial) == 1) {
							Integer all_part =1;
							Integer all=0;
							Integer cano_part=0;
							Integer cano=0;
							if (containsMention(title_body_anchor,
									label_name_redirect_canonical) == 1){
								 all=1;
							}
							if (containsMention(title_body_anchor,
									canonical_partial) == 1){
								 cano_part =1;
							}
							if (containsMention(title_body_anchor,
									canonical) == 1){
								 cano =1;
							}



							double temp = 0.0;

							for (String partial : partialtopics.get(topic)) {
								if (containsMention(
										title_body_anchor,
										new HashSet<String>(Arrays
												.asList(partial))) == 1) {
									temp = temp + (double) partial.length()
											/ topic_head.length();
								}

							}
							temp = temp * scale;
							all_attr.put("partial", new Double(temp).toString());
							temp = 0.0;
							// Log.info("What is failing here: topic +topic head:"
							// + topic
							// + topic_head);
							for (String s_form : DbPedia.get(topic)) {
								if (containsMention(
										title_body_anchor,
										new HashSet<String>(Arrays
												.asList(s_form))) == 1) {
									temp = temp + (double) s_form.length()
											/ topic_head.length();
								}

							}

							temp = temp * scale;

							all_attr.put("s_form", new Double(temp).toString());

							temp = 0.0;
							if (!topic.startsWith("https://twitter.com/")) {
								// Log.info("See from gcld:" + topic);
								if (Gcld.get(topic_head) != null) {
									for (String gcld_key : Gcld.get(topic_head)
											.keySet()) {
										if (containsMention(
												title_body_anchor,
												new HashSet<String>(Arrays
														.asList(gcld_key))) == 1) {
											temp = temp
													+ Gcld.get(topic_head).get(
															gcld_key);
										}

									}
								}

								temp = temp * scale;

								all_attr.put("gcld",
										new Double(temp).toString());

								temp = 0.0;
								if (Ppr.get(topic_head) != null) {
									for (String ppr_key : Ppr.get(topic_head)
											.keySet()) {
										if (containsMention(
												title_body_anchor,
												new HashSet<String>(Arrays
														.asList(ppr_key))) == 1) {
											temp = temp
													+ Ppr.get(topic_head).get(
															ppr_key);
										}

									}
								}
								temp = temp * scale;

								all_attr.put("ppr", new Double(temp).toString());

								temp = 0.0;

								if (ContextFreqsL.containsKey(topic_head)) {
									for (String contxL_key : ContextFreqsL.get(
											topic_head).keySet()) {
										if (containsMention(
												title_body_anchor,
												new HashSet<String>(Arrays
														.asList(contxL_key))) == 1) {
											temp = temp
													+ ContextFreqsL.get(
															topic_head).get(
															contxL_key);
										}

									}
									temp = temp * scale;

									all_attr.put("contxL",
											new Double(temp).toString());

									temp = 0.0;
								}
								if (ContextFreqsR.containsKey(topic_head)) {
									for (String contxR_key : ContextFreqsR.get(
											topic_head).keySet()) {
										if (containsMention(
												title_body_anchor,
												new HashSet<String>(Arrays
														.asList(contxR_key))) == 1) {
											temp = temp
													+ ContextFreqsR.get(
															topic_head).get(
															contxR_key);
										}

									}
									temp = temp * scale;

									all_attr.put("contxR",
											new Double(temp).toString());

									temp = 0.0;
								}

								/****************************
								 * Add some similarity metrics
								 * 
								 */

								HashMap<String, Integer> doc_map = new HashMap<String, Integer>();
								HashMap<String, Integer> wiki_map = new HashMap<String, Integer>();
								doc_map = ReturnFreqs(title_body_anchor);
								wiki_map = ReturnFreqs(WikiContent
										.get(topic_head));
								// Add cosine similarity
								temp = cosine_similarity(wiki_map, doc_map);

								all_attr.put("cos", new Double(temp).toString());

								temp = 0.0;
								// Add kl -diveregence
								temp = klDivergence(wiki_map, doc_map);

								all_attr.put("kl", new Double(temp).toString());

								temp = 0.0;

								// Add Jaccard
								temp = jaccardSimilarity(wiki_map.keySet(),
										doc_map.keySet());

								all_attr.put("jac", new Double(temp).toString());

								temp = 0.0;
							}

							Integer firstPos = returnFirstIndex(body,
									DbPedia.get(topic));
							Integer lastPos = returnLastIndex(body,
									DbPedia.get(topic));

							Integer spread = lastPos - firstPos;
							Integer firstPosNorm = firstPos.intValue()
									/ (lengthBody.intValue() + 1);
							Integer lastPosNorm = lastPos.intValue()
									/ (lengthBody.intValue() + 1);
							Integer spreadNorm = spread
									/ (lengthBody.intValue() + 1);
							all_attr.put("FirstPos", firstPos.toString());
							all_attr.put("LastPos", lastPos.toString());
							all_attr.put("Spread", spread.toString());
							all_attr.put("FirstPosNorm",
									firstPosNorm.toString());
							all_attr.put("LastPosNorm", lastPosNorm.toString());
							all_attr.put("SpreadNorm", spreadNorm.toString());
							//Log.info("keneth hood:" + title + "\t" +DbPedia.get(topic));
							all_attr.put("MentionsTitle",
									frequencyCounter(title, DbPedia.get(topic)));
							all_attr.put("MentionsBody",
									frequencyCounter(body, DbPedia.get(topic)));

							all_attr.put(
									"MentionsAnchor",
									frequencyCounter(anchor, DbPedia.get(topic)));

							

							String all_values = "";
							for (String key1 : all_attr.keySet()) {
								if(key1.equalsIgnoreCase("RelatedBody")){
									break;
								}
								
								if (all_values.equalsIgnoreCase("")) {
									all_values = all_attr.get(key1);
								} else {
									all_values = all_values + ","
											+ all_attr.get(key1);
								}
								
							}
							
							all_values = all_values + ","
									+ all_attr.get("Class");
							
						String[] key_split = key.toString().split("/");
						
						//if(TrainTest.containsKey(streamid)) { //&& TrainTest.get(streamid).equalsIgnoreCase(key.toString())){ 
							String columns = "CWI" + "\t" + "two-step" + "\t"
									+ streamid + "\t" + topic +"\t" +source+"\t"+ all_part.toString()+"\t"+ all.toString()+"\t"+ cano_part.toString()+"\t"+ cano.toString()+ "\t" + "1000" + "\t"
									+ "2" + "\t" + "1" + "\t"
									+ key+ "\t" + "NULL"
									+ "\t" + "-1" + "\t" + "0-0" + "\t";
						if (anno_value != -3) {	
							context.write(new Text(columns), new Text(all_values));
							 
						}

						}
					}

				}
			}
		}

		public int containsMention(String str, HashSet<String> hs) {
			int i = 0;
			for (String entity : hs) {
				if (str.contains(entity)) {
					i = 1;
				}
			}
			return i;
		}
		public List <String> containsMention(List<String> ls) {
			
			for (int i=0; i<ls.size(); i++){
				if (ls.get(i).length()<3){
					ls.remove(i);
				}
			}
			return ls;
		}
		public  List <String> listremove(List<String> ls) {
			
			Iterator<String> i = ls.iterator();
			//List <List> lst2 =new ArrayList <String>();
			List<String> list = new ArrayList<String>();
			while (i.hasNext()) {
			   String s = i.next(); // must be called before you can call i.remove()
			  s =s.replace("(", "").replace(")", "");
			   if (s.length()>2){
				   list.add(s);	      
			   }
			   }
			
			return list;
}
		
		
		public int returnFirstIndex(String str, HashSet<String> hs) {

			int f_index = 0;
			for (String entity : hs) {
				if (str.indexOf(entity) > f_index) {
					f_index = str.indexOf(entity);

				}
			}
			return f_index;

		}

		public int returnLastIndex(String str, HashSet<String> hs) {

			int l_index = 0;
			for (String entity : hs) {
				if (str.lastIndexOf(entity) > l_index) {
					l_index = str.lastIndexOf(entity);
				}
			}
			return l_index;

		}

		public String frequencyCounter(String str, HashSet<String> hs) {
			Integer counter = 0;
			for (String entity : hs) {
				
				try {
				Pattern pattern = Pattern.compile(entity);
				Matcher matcher = pattern.matcher(str);
				
				while (matcher.find())
					counter++;
				}
				catch (PatternSyntaxException e) {
					//Log.info("what what what "+ entity);
				}
			}
			return counter.toString();
		}

		public String entityMentionCounter(String str) {
			Integer rel = 0;
			for (String topic : topicregexes.keySet()) {

				if (containsMention(str, DbPedia.get(topic)) == 1) {
					// Log.info("Null pointer" + str + " " +
					// DbPedia.get(topic));
					rel++;

				}

			}
			return rel.toString();

		}

		private void loadTopics(String xcontext) {

			DataInputStream in = null;
			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				ft = new Filter_topics.Factory().loadTopics(br);
				//LOG.info(ft.getTopic_set_id());
				Targets[] t = ft.getTargets();
				Iterator it = Arrays.asList(t).iterator();
				// for (Target t : Arrays.asList(t))) {
				while (it.hasNext()) {
					Targets targ = (Targets) it.next();
					Pattern p;

					// add the full name
					p = Pattern.compile(
							".*\\b+" + targ.target_id.replaceAll("_", " ")
									+ "\\b+.*", Pattern.CASE_INSENSITIVE);
					topicregexes.put(targ.target_id, p);

					// add the individual terms
					HashSet<String> pset = new HashSet<String>();
					pset.addAll(new HashSet<String>(Arrays
							.asList(targ.target_id.split("_"))));
					pset.add(targ.target_id.replaceAll("_", " "));
					partialtopics.put(targ.target_id, pset);

				}

			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: read instances");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: " + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
		}

		private Map<String, HashSet<String>> loadLabels(String xcontext) {
			// Hashtable<String, Hashtable> prep = new Hashtable();
			Map<String, HashSet<String>> prep = new LinkedHashMap<String, HashSet<String>>();
			Hashtable<String, HashSet<String>> tmp = new Hashtable<String, HashSet<String>>();
			DataInputStream in = null;

			// System.out.println("This is from LoadLabels");
			// System.out.println("\nwhat am I opening :" + xcontext);
			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				// Read File Line By Line
				while ((strLine = br.readLine()) != null) {
					// Print the content on the console
					String temp_key;
					String temp_value;

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");

					temp_key = str[0];

					temp_value = str[1];

					// System.out.println("length " + str.length);
					HashSet<String> keys = (HashSet<String>) tmp.get(temp_key);
					if (keys == null) {
						keys = new HashSet<String>();
						tmp.put(temp_key, keys);
					}
					keys.add(temp_value.toLowerCase());
					// keys.add(temp_value);
				}
				br.close();
				for (Object entity : tmp.keySet()) {
					prep.put((String) entity, tmp.get(entity));

				}

			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("" + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
			return prep;
		}

		/***
		 * This method takes an annotation file and returns a map whose key is
		 * doc-entity and its value the annotation value assigned. In case there
		 * is multiple annotations, it takes the lowest annotation value
		 * 
		 * @param xcontext
		 * @return
		 */
		private Map<String, Integer> loadAnnotation(String xcontext) {
			Map<String, Integer> prep = new LinkedHashMap<String, Integer>();
			DataInputStream in = null;

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				// Read File Line By Line
				while ((strLine = br.readLine()) != null) {
					// Print the content on the console
					String temp_key;
					String temp_value;

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");

					temp_key = str[2] + "_" + str[3];
					temp_value = str[5];

					// if (Integer.parseInt(str[6]) == 1) {
					if (prep.containsKey(temp_key)) {
						if (prep.get(temp_key).intValue() > Integer
								.parseInt(temp_value)) {
							prep.put(temp_key, Integer.parseInt(temp_value));
						}
					} else {
						prep.put(temp_key, Integer.parseInt(temp_value));
					}
					// }

				}
				br.close();

			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: read instances");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("File not found: " + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
			return prep;
		}

		private Map<String, String> loadTrainTest(String xcontext) {
			// Hashtable<String, Hashtable> prep = new Hashtable();
			Map<String, String> prep = new LinkedHashMap<String, String>();

			DataInputStream in = null;

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				// Read File Line By Line

				while ((strLine = br.readLine()) != null) {
					String temp_key;

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");

					temp_key = str[0];

					// int i = 0;
					// temp_key = entity[i];
					// while (++i < entity.length) {
					// temp_key = temp_key + "_" + entity[i];
					// }
					//
					// if (temp_key.equalsIgnoreCase("William_H._Gates,_Sr.")) {
					// temp_key = "William_H._Gates,_Sr";
					// }
					// Log.info("From Load Wiki "+ temp_key);
					prep.put(temp_key, str[1]);

				}
				br.close();

			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: read instances");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: " + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

			}

			return prep;
		}

		
		private Map<String, HashMap<String, Double>> loadPPR(String xcontext) {
			// Hashtable<String, Hashtable> prep = new Hashtable();
			Map<String, HashMap<String, Double>> prep = new LinkedHashMap<String, HashMap<String, Double>>();

			DataInputStream in = null;

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				// Read File Line By Line

				String key_check = "";
				HashMap<String, Double> intermediat = new HashMap<String, Double>();

				while ((strLine = br.readLine()) != null) {
					String temp_key;
					Double temp_value;
					String temp_str;

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");

					String[] entity = str[1].split(" ");
					int i = 0;
					temp_key = entity[i];
					while (++i < entity.length) {
						temp_key = temp_key + "_" + entity[i];
					}

					temp_str = str[3].toLowerCase();
					// temp_str = str[3];
					if (key_check == "") {
						key_check = temp_key;
						// System.out.println( " Executed ! :) "+ key_check );
					}
					temp_value = Double.parseDouble(str[4]);

					if (!key_check.equalsIgnoreCase(temp_key)) {
						// System.out.println(" When: " + temp_key);
						if (key_check.equalsIgnoreCase("William_H._Gates,_Sr.")) {
							key_check = "William_H._Gates,_Sr";
						}
						HashMap<String, Double> temp = new HashMap<String, Double>();
						temp.putAll(intermediat);
						prep.put(key_check, temp);
						intermediat.clear();
						key_check = temp_key;
					}

					if (intermediat.containsKey(temp_str)) {
						intermediat.put(temp_str, intermediat.get(temp_str)
								+ temp_value);
					} else {
						intermediat.put(temp_str, temp_value);
					}

				}
				if (key_check.equalsIgnoreCase("William_H._Gates,_Sr.")) {
					key_check = "William_H._Gates,_Sr";
				}
				prep.put(key_check, intermediat);
				br.close();

			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: read instances");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: " + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

			}

			return prep;
		}

		private Map<String, String> loadWiki(String xcontext) {
			// Hashtable<String, Hashtable> prep = new Hashtable();
			Map<String, String> prep = new LinkedHashMap<String, String>();

			DataInputStream in = null;

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				// Read File Line By Line

				while ((strLine = br.readLine()) != null) {
					String temp_key;

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");

					temp_key = str[0];

					// int i = 0;
					// temp_key = entity[i];
					// while (++i < entity.length) {
					// temp_key = temp_key + "_" + entity[i];
					// }
					//
					// if (temp_key.equalsIgnoreCase("William_H._Gates,_Sr.")) {
					// temp_key = "William_H._Gates,_Sr";
					// }
					// Log.info("From Load Wiki "+ temp_key);
					prep.put(temp_key, str[3]);

				}
				br.close();

			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: read instances");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: " + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

			}

			return prep;
		}

		private Map<String, HashMap<String, Double>> loadGCLD(String xcontext) {
			Map<String, HashMap<String, Double>> prep = new LinkedHashMap<String, HashMap<String, Double>>();

			DataInputStream in = null;

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				String key_check = "";

				HashMap<String, Double> intermediat = new HashMap<String, Double>();

				while ((strLine = br.readLine()) != null) {

					String temp_key;
					Double temp_value;
					String temp_str;

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");

					temp_str = str[0].toLowerCase();
					// temp_str = str[0];
					temp_key = str[2];
					temp_value = Double.parseDouble(str[1])
							* Double.parseDouble(str[4]);
					if (key_check == "") {
						key_check = temp_key;
						// System.out.println( " Executed ! :) "+ key_check );
					}

					if (!key_check.equalsIgnoreCase(temp_key)) {
						// System.out.println(" When ! :) " + temp_key);
						HashMap<String, Double> temp = new HashMap<String, Double>();
						temp.putAll(intermediat);
						prep.put(key_check, temp);
						intermediat.clear();
						key_check = temp_key;
					}
					if (intermediat.containsKey(temp_str)) {
						intermediat.put(temp_str, intermediat.get(temp_str)
								+ temp_value);
					} else {
						intermediat.put(temp_str, temp_value);
					}
					// lastValue_Holder = intermediat;

				}
				prep.put(key_check, intermediat);
				br.close();

			} catch (IOException e) {
				e.printStackTrace();
				// LOG.error("read from distributed cache: read instances");
			} catch (Exception e) {
				e.printStackTrace();
				// LOG.error("read from distributed cache: " + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

			}
			return prep;
		}

		private Map<String, String> loadMyVer(String xcontext) {
			// Hashtable<String, Hashtable> prep = new Hashtable();
			Map<String, String> prep = new HashMap<String, String>();

			DataInputStream in = null;

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;
				while ((strLine = br.readLine()) != null) {

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");
					prep.put(str[0], str[1]);
				}
				br.close();

			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: read instances");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: " + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

			}
			return prep;
		}

		private void loadContext(String xcontext) {
			DataInputStream in = null;

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;
				while ((strLine = br.readLine()) != null) {

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");

					String temp_str1 = str[1].toLowerCase();
					String temp_str2 = str[2].toLowerCase();
					/*
					 * String temp_str1 = str[1]; String temp_str2 = str[2];
					 */

					if (ContextL.containsKey(str[0])) {
						// System.out.println(Context.get(str[0]));
						ContextL.put(str[0], ContextL.get(str[0]) + "==="
								+ temp_str1);
						ContextR.put(str[0], ContextR.get(str[0]) + "==="
								+ temp_str2);

					} else {
						ContextL.put(str[0], temp_str1);
						ContextR.put(str[0], temp_str2);

					}

				}
				br.close();

			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: read instances");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("read from distributed cache: " + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

			}

		}

		/****************************************************************/
		private HashMap<String, Integer> ReturnFreqs(String str) {

			HashMap<String, Integer> temp = new HashMap<String, Integer>();
			try {
				NGramExtractor extractor = new NGramExtractor();

				// get the uni-grams

				extractor.extract(str, 1, 1, true);

				LinkedList<String> ngrams = extractor.getUniqueNGrams();

				for (String s : ngrams) {

					if (temp.containsKey(s)) {
						temp.put(s,
								temp.get(s) + extractor.getNGramFrequency(s));
					} else {
						temp.put(s, extractor.getNGramFrequency(s));
					}
				}

				// Repeat it for ngarsms

				extractor.extract(str.toString(), 2, 4, false);

				ngrams = extractor.getUniqueNGrams();

				for (String s : ngrams) {
					// System.out.println( s + " \t\t " +
					// extractorL.getNGramFrequency(s) );
					if (temp.containsKey(s)) {
						temp.put(s,
								temp.get(s) + extractor.getNGramFrequency(s));
					} else {
						temp.put(s, extractor.getNGramFrequency(s));
					}
				}

			} catch (Exception e) {
				// System.err.println("Did Not work: " + e.toString());
				e.printStackTrace();
			}
			return temp;

		}

		/*************************************************************/
		private Double cosine_similarity(Map<String, Integer> v1,
				Map<String, Integer> v2) {
			Set<String> both = Sets.newHashSet(v1.keySet());
			both.retainAll(v2.keySet());
			double sclar = 0.0, norm1 = 0.0, norm2 = 0.0;
			for (String k : both)
				sclar += v1.get(k).doubleValue() * v2.get(k).doubleValue();
			for (String k : v1.keySet())
				norm1 += v1.get(k).doubleValue() * v1.get(k).doubleValue();
			for (String k : v2.keySet())
				norm2 += v2.get(k).doubleValue() * v2.get(k).doubleValue();
			return sclar / Math.sqrt(norm1 * norm2);
		}

		/**************************************/

		private Double klDivergence(Map<String, Integer> v1,
				Map<String, Integer> v2) {
			double log2 = Math.log(2);

			double klDiv = 0.0;

			for (String k : v1.keySet()) {
				if (v1.get(k) == 0) {
					continue;
				}
				if (v2.containsKey(k))
					if (v2.get(k) == 0) {
						continue;
					} // Limin
					else {
						;
					}
				else {
					continue;
				}

				klDiv += v1.get(k).doubleValue()
						* Math.log(v1.get(k).doubleValue()
								/ v2.get(k).doubleValue());
			}

			klDiv = klDiv / log2; // moved this division out of the loop -DM
			/*
			 * if (klDiv == Double.POSITIVE_INFINITY || klDiv ==
			 * Double.NEGATIVE_INFINITY) klDiv = 0;
			 */
			return klDiv;
		}

		/********************/

		public double jaccardSimilarity(Set<String> h1, Set<String> h2) {

			int sizeh1 = h1.size();
			// Retains all elements in h3 that are contained in h2 ie
			// intersection
			h1.retainAll(h2);
			// h1 now contains the intersection of h1 and h2
			// System.out.println("Intersection " + h1);

			h2.removeAll(h1);
			// h2 now contains unique elements
			// System.out.println("Unique in h2 " + h2);

			// Union
			int union = sizeh1 + h2.size();
			int intersection = h1.size();

			return (double) intersection / union;

		}

		/**************************************************************************/
		private void extractNgrmFrequency() {

			for (String k : ContextL.keySet()) {
				// System.out.println("This is the id: \n" + ContextL.get(k));
				HashMap<String, Integer> tempL = new HashMap<String, Integer>();
				HashMap<String, Integer> tempR = new HashMap<String, Integer>();

				String[] leftContexts = ContextL.get(k).split("===");
				String[] rightContexts = ContextR.get(k).split("===");

				for (int i = 0; i < leftContexts.length; i++) {

					tempL.putAll(ReturnFreqs(leftContexts[i]));
					tempR.putAll(ReturnFreqs(rightContexts[i]));
				}

				ContextFreqsL.put(k, tempL);
				ContextFreqsR.put(k, tempR);
			}

			// Only keep the 10 best from each n-gram
			int top_n = 5;
			Map<String, HashMap<String, Integer>> tempL = new LinkedHashMap<String, HashMap<String, Integer>>();
			Map<String, HashMap<String, Integer>> tempR = new LinkedHashMap<String, HashMap<String, Integer>>();

			for (String k : ContextFreqsR.keySet()) {
				List<Integer> l = new ArrayList<Integer>(ContextFreqsR.get(k)
						.values());
				Collections.sort(l);
				int leng = l.size();
				List<Integer> top_values = l.subList(leng - top_n, leng);
				HashMap<String, Integer> hs = new HashMap<String, Integer>();
				for (String k2 : ContextFreqsR.get(k).keySet()) {
					if (top_values.contains(ContextFreqsR.get(k).get(k2))) {

						hs.put(k2, ContextFreqsR.get(k).get(k2));

					}
				}
				tempR.put(k, hs);
			}

			for (String k : ContextFreqsL.keySet()) {
				List<Integer> l = new ArrayList<Integer>(ContextFreqsL.get(k)
						.values());
				Collections.sort(l);
				int leng = l.size();
				List<Integer> top_values = l.subList(leng - top_n, leng);
				HashMap<String, Integer> hs = new HashMap<String, Integer>();
				for (String k2 : ContextFreqsL.get(k).keySet()) {
					if (top_values.contains(ContextFreqsL.get(k).get(k2))) {

						hs.put(k2, ContextFreqsL.get(k).get(k2));

					}
				}
				tempL.put(k, hs);
			}

			ContextFreqsR = tempR;
			ContextFreqsL = tempL;

		}

	}

	/**
	 * Loads the JSON topic file.
	 * 
	 * @param context
	 */

	private static void loadTopicData(String queryfile, Run_info fr,
			FileSystem fs, HashMap<String, Object> run_info) {

		FSDataInputStream in = null;
		try {

			in = fs.open(new Path(queryfile));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			Filter_topics ft = new Filter_topics.Factory().loadTopics(br);

			fr.setTopic_set_id(ft.getTopic_set_id());
			run_info.put("num_entities", ft.getTargets().length);

		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {

			if (in != null) {
				try {
					in.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new KbaNameVariantMatchFeatureExtractor(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ KbaNameVariantMatchFeatureExtractor.class.getName()
						+ " -i input \n"
						+ "-o output\n "
						+ "-q query_file (hdfs) \n"
						+ "-l labels file\n "
						+ "-a annotation file \n"
						+ "-p ppr file \n"
						+ "-g gcld file \n"
						+ "-c context File \n"
						+ "-w wiki File\n \n"
						+ "[-c corpus_id -r runtag -t teamname -d description -ds description short] \n\n "
						+ "Example usage: hadoop jar trec-kba.jar "
						+ KbaNameVariantMatchFeatureExtractor.class.getName()
						+ " "
						+ "-i kba/tiny-kba-stream-corpus/*/* "
						+ "-i KBA/Data/kba-stream-corpus-2012-cleansed-only-testing-out/*/*"
						+ "-o KBA/OutPut/Kba_2012/testing_features_normalized.topic.ppr.gcld.sfrom.cl.cr.cos.kl.jac "
						+ "-q KBA/Data/trec-kba-ccr-2012.filter-topics.json"
						+ "-l KBA/Data/entity-surface-forms_merged.txt"
						+ "-a KBA/Data/trec-kba-ccr-2012-judgments-2012JUN22-final.filter-run.txt"
						+ "-p KBA/Data/trec-kba-ccr-topics-2012.ppr-top100.txt "
						+ "-g KBA/Data/dictionary.txt"
						+ "-c KBA/Data/wikilink-context.txt"
						+ "-w KBA/Data/trec-kba-ccr-topics-2012-wiki.txt\n\n");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;
		String queryfile = null;
		String contextFile = null;
		String systemdescription = null;
		String systemdescription_short = null;
		String corpus_id = null;
		String runtag = null;
		String teamname = null;
		String annoFile = null;
		String gcldFile = null;
		String labelsFile = null;
		String pprFile = null;
		String myverFile = null;
		String wikiFile = null;
		String traintestFile = null;
		HashMap<String, Object> run_info = new HashMap<String, Object>();

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					in = args[++i];
				} else if ("-o".equals(args[i])) {
					out = args[++i];
				} else if ("-q".equals(args[i])) {
					queryfile = args[++i];
				} else if ("-r".equals(args[i])) {
					runtag = args[++i];
				} else if ("-l".equals(args[i])) {
					labelsFile = args[++i];
				} else if ("-a".equals(args[i])) {
					annoFile = args[++i];
				} else if ("-t".equals(args[i])) {
					teamname = args[++i];
				} else if ("-d".equals(args[i])) {
					systemdescription = args[++i];
				} else if ("-ds".equals(args[i])) {
					systemdescription_short = args[++i];
				} else if ("-p".equals(args[i])) {
					pprFile = args[++i];
				} else if ("-g".equals(args[i])) {
					gcldFile = args[++i];

				} else if ("-s".equals(args[i])) {
					myverFile = args[++i];

				} else if ("-c".equals(args[i])) {
					contextFile = args[++i];
				} else if ("-w".equals(args[i])) {
					wikiFile = args[++i];
				} else if ("-tt".equals(args[i])) {
					traintestFile = args[++i];
				} else if ("-h".equals(args[i]) || "--help".equals(args[i])) {
					return printUsage();
				} else {
					other_args.add(args[i]);
				}
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[i - 1]);
				return printUsage();
			}
		}

		if (other_args.size() > 0 || in == null || out == null
				|| queryfile == null)
			return printUsage();

		if (runtag == null)
			runtag = "toy_1";

		if (teamname == null)
			teamname = "CompInsights";

		if (corpus_id == null)
			corpus_id = "kba-stream-corpus-2012-cleansed-only";

		if (systemdescription == null)
			systemdescription = "Description intentionally left blank.";
		if (systemdescription_short == null)
			systemdescription_short = "a two -step classification approach";

//		LOG.info("Tool: " + this.getClass().getName());
//		LOG.info(" - input path: " + in);
//		LOG.info(" - output path: " + out);
//		LOG.info(" - runtag: " + runtag);
//		LOG.info(" - teamname: " + teamname);
//		LOG.info(" - corpus_id: " + corpus_id);
//		LOG.info(" - run description: " + systemdescription);

		Run_info fr = new Run_info.Factory().create(teamname, runtag,
				systemdescription, systemdescription_short, corpus_id);

		Map<String, String> Attr = new LinkedHashMap<String, String>();
		// Attr.put("trec-kba", "");
		/*
		 * Attr.put("LengthTitle", ""); Attr.put("LengthBody", "");
		 * Attr.put("LengthAnchor", ""); Attr.put("Source", "");
		 * Attr.put("English", ""); Attr.put("MentionsTitle", "");
		 * Attr.put("MentionsBody", ""); Attr.put("MentionsAnchor", "");
		 * Attr.put("FirstPos", ""); Attr.put("LastPos", ""); Attr.put("Spread",
		 * ""); Attr.put("FirstPosNorm", ""); Attr.put("LastPosNorm", "");
		 * Attr.put("SpreadNorm", ""); // Attr.put("Related", "");
		 * Attr.put("Relatedtitle", ""); Attr.put("RelatedBody", "");
		 * Attr.put("RelatedAnchor", ""); Attr.put("ppr", ""); Attr.put("gcld",
		 * ""); Attr.put("partial", ""); Attr.put("s_form", "");
		 * Attr.put("contxL", "0"); Attr.put("contxR", "0"); Attr.put("cos",
		 * "0"); Attr.put("kl", "0"); Attr.put("jac", "0"); Attr.put("Class",
		 * "");
		 */
		Attr.put("gcld", "0");
		Attr.put("jac", "0");
		Attr.put("cos", "0");
		Attr.put("kl", "0");
		Attr.put("ppr", "0");
		Attr.put("s_form", "0");
		Attr.put("contxR", "0");
		Attr.put("contxL", "0");
		Attr.put("FirstPos", "0");
		Attr.put("LastPos", "0");
		Attr.put("LengthBody", "0");
		Attr.put("FirstPosNorm", "0");
		Attr.put("MentionsBody", "0");
		Attr.put("RelatedBody", "0");
		Attr.put("Spread", "0");
		Attr.put("LastPosNorm", "0");
		Attr.put("SpreadNorm", "0");
		Attr.put("LengthAnchor", "0");
		Attr.put("Source", "0");
		Attr.put("LengthTitle", "0");
		Attr.put("partial", "0");
		Attr.put("MentionsAnchor", "0");
		Attr.put("Relatedtitle", "0");
		Attr.put("English", "0");
		Attr.put("RelatedAnchor", "0");
		Attr.put("MentionsTitle", "0");
		Attr.put("Class", "0");

		Configuration conf = getConf();
		conf.set(QUERYFILEPATH_HDFS, new Path(queryfile).toUri().toString());
		conf.set(LABELSFILEPATH_HDFS, new Path(labelsFile).toUri().toString());
		conf.set(ANNOFILEPATH_HDFS, new Path(annoFile).toUri().toString());
		conf.set(PPR_HDFS, new Path(pprFile).toUri().toString());
		conf.set(MYVER, new Path(myverFile).toUri().toString());
		conf.set(GCLD_HDFS, new Path(gcldFile).toUri().toString());
		conf.set(CONTEXT_HDFS, new Path(contextFile).toUri().toString());
		conf.set(WIKI_HDFS, new Path(wikiFile).toUri().toString());
		conf.set(TrainTest_HDFS, new Path(traintestFile).toUri().toString());
		conf.set(RUNTAG, runtag);
		conf.set(TEAMNAME, teamname);

		// set time
		conf.setLong("mapred.task.timeout", 40 * 600000);
		conf.set("mapred.map.child.java.opts", "-Xmx4g -XX:-UseGCOverheadLimit");
		conf.set("mapred.child.java.opts", "-Xmx4096m");

		FileSystem fs = FileSystem.get(conf);
		// Lookup required data from the topic file
		loadTopicData(queryfile, fr, fs, run_info);
		Job job = new Job(conf, "Feature Extractor");
		job.setJarByClass(KbaNameVariantMatchFeatureExtractor.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.
		DistributedCache.addCacheFile(new URI(new Path(queryfile) + "#"
				+ QUERYFILEPATH_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(labelsFile) + "#"
				+ LABELSFILEPATH_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(annoFile) + "#"
				+ ANNOFILEPATH_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(pprFile) + "#"
				+ PPR_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(gcldFile) + "#"
				+ GCLD_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(contextFile) + "#"
				+ CONTEXT_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(wikiFile) + "#"
				+ WIKI_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(traintestFile) + "#"
				+ TrainTest_HDFS), job.getConfiguration());

		DistributedCache.addCacheFile(
				new URI(new Path(myverFile) + "#" + MYVER),
				job.getConfiguration());

		DistributedCache.createSymlink(job.getConfiguration());
		/*
		job.setInputFormatClass(ThriftFileInputFormat.class);
		job.setMapperClass(MyMapper.class);
		FileInputFormat.addInputPath(job, new Path(in));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// job.setCombinerClass(MyReducer.class);
		// job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);

		FileSystem.get(conf).delete(new Path(out), true);
		TextOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Let's go
		int status = job.waitForCompletion(true) ? 0 : 1;
		*/
		
		//job.setInputFormatClass(ThriftFileInputFormat.class);
		//job.setInputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setMapperClass(MyMapper.class);
	    FileInputFormat.addInputPath(job, new Path(in));

	    //job.setMapOutputKeyClass(Text.class);
	    //job.setMapOutputValueClass(StreamItemWritable.class);
	    //job.setMapOutputValueClass(Text.class);
	    //job.setOutputKeyClass(StreamItemWritable.class);

	    // job.setCombinerClass(MyReducer.class);
	    //job.setReducerClass(MyReducer.class);
	    job.setNumReduceTasks(1);

	    FileSystem.get(conf).delete(new Path(out), true);
	    SequenceFileOutputFormat.setOutputPath(job, new Path(out));
	    //job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    //job.setOutputValueClass(StreamItemWritable.class);
	    job.setOutputValueClass(Text.class);
	    //LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
	    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	   

	    // Let's go
	    int status = job.waitForCompletion(true) ? 0 : 1;

	    
	    
		Counters c = job.getCounters();

		long cputime = c.findCounter(
				org.apache.hadoop.mapred.Task.Counter.CPU_MILLISECONDS)
				.getValue();
		run_info.put("elapsed_time", ((double) cputime));

		long num_filter_results = c.findCounter(
				org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS)
				.getValue();
		run_info.put("num_filter_results", num_filter_results);

		long num_entity_doc_compares = c.findCounter(
				org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS)
				.getValue();
		run_info.put("num_entity_doc_compares", num_entity_doc_compares);

		long hours = c.findCounter(
				org.apache.hadoop.mapred.Task.Counter.REDUCE_INPUT_GROUPS)
				.getValue();
		run_info.put("num_stream_hours", hours);

		fr.setAdditionalProperties("run_info", run_info);

		System.out.println("#" + new Run_info.Factory().toJSON(fr));
		System.out.println("@RELATION" + " trec-kba" + " ");
		for (String key : Attr.keySet()) {
			if (key.equalsIgnoreCase("English")) {
				System.out.println("@ATTRIBUTE " + key + " " + "{0,1,2}");
			} else if (key.equalsIgnoreCase("Class")) {
				System.out.println("@ATTRIBUTE " + key + " " + "{0,1}");
			} else {
				System.out.println("@ATTRIBUTE " + key + " " + "NUMERIC");
			}

		}
		System.out.println("\n@DATA");
		// Text line = new Text();
		// LineReader reader = new LineReader(fs.open(new Path(out
		// + "/part-r-00000")));
		// for (int i = 0; i < num_filter_results; i++) {
		// reader.readLine(line);
		// System.out.println(line.toString().split("\t\t")[1]);
		// }

		System.out.println("#"
				+ new Run_info.Factory().toPrettyJSON(fr).replaceAll("\\n",
						"\n#"));

		return status;

	}
}
