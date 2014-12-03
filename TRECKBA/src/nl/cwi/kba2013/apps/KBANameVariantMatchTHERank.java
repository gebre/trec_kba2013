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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import nl.cwi.json2013.topics.Filter_topics;
import nl.cwi.json2013.topics.Targets;
import nl.cwi.kba2013.thrift.ContentItem;
import nl.cwi.kba2013.thrift.bin.StreamItemWritable;
import nl.cwi.kba2013.thrift.bin.ThriftFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;


/**
 * TThis program extracts features from either the training set or test set.
 * documents based on mere lexical matching.
 * 
 * @author Gebremeskel
 * 
 */
public class KBANameVariantMatchTHERank extends Configured implements Tool {

	public static final String QUERYFILEPATH_HDFS = "kba.topicfilelocation";
	
	public static final String TrainTest_HDFS = "tt";
	public static final String LABELSFILEPATH_HDFS = "kba.labels";
	
	public static Map<String, HashSet<String>> DbPedia = new LinkedHashMap<String, HashSet<String>>();
	public static Map<String,  String> TrainTest = new LinkedHashMap<String, String>();
	// public static Map<String, String> Attr ;

	protected enum Counter {
		documents, doc_with_body, body_raw, body_with_clean_html, body_with_clean_vissible
	};

	private static final Logger LOG = Logger
			.getLogger(KBANameVariantMatchTHERank.class);

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		//private static final Text out = new Text();
		//private static final Text none = new Text("");

		

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Integer a_p=0, a=0, c_p=0, c=0, l=0;
			for(Text value:values){
				String[] v_s=value.toString().split("\t");
				a_p =a_p+Integer.parseInt(v_s[0]); 
				a =a+Integer.parseInt(v_s[1]); 
				c_p =c_p+Integer.parseInt(v_s[2]); 
				c =c+Integer.parseInt(v_s[3]); 
				l =l+Integer.parseInt(v_s[4]); 
				
			}
			
			context.write(key, new Text(a_p.toString()+"\t"+a.toString()+"\t"+c_p.toString()+"\t"+c.toString()+"\t"+l.toString()));

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

	
		public static final int scale = 1000;
		// private static final Text featurLine = new Text();
		//private Map<String, Pattern> topicregexes = new LinkedHashMap<String, Pattern>();
		//private Map<String, HashSet<String>> partialtopics = new LinkedHashMap<String, HashSet<String>>();
		private Set<String> topicSet = new LinkedHashSet<String>();
		private Filter_topics ft = null;

		/**
		 * Used to load the queries.
		 */
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			
			

			DbPedia = loadLabels(LABELSFILEPATH_HDFS);
			topicSet=DbPedia.keySet();

			//TrainTest = loadTrainTest(TrainTest_HDFS);
//			

			//loadTopics(QUERYFILEPATH_HDFS);

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


			
			//Log.info("watch streamid "+key.toString().split("/")[7]);
			if (value.getBody() != null) {
			
//				if (value.getBody().getRaw()!=null){
//				//if (value.getBody().getClean_visible() != null){
//					String body = new String () ;
//					if (value.getBody().getRaw().length != 0){
//						
//						//byte [] encoded =Base64.encodeBase64(value.getBody().getRaw());
//						body = new String(value.getBody().getRaw(), "UTF-8");
//						//Log.info("raw body:" +body);
//												
//					}
					

					//if (value.getBody().getRaw()!=null){
					if (value.getBody().getClean_visible() != null){
						String body = new String () ;
//						ContentItem b = value.getBody();
//						byte[] r = b.getRaw();
//						String encoding;
//						if (b.getEncoding()!=null){
//							encoding=b.getEncoding();
//						}
//						else{
//							encoding="UTF-8";
//						}
//						if (r.length != 0){
//							
//							//byte [] encoded =Base64.encodeBase64(value.getBody().getRaw());
//							body = new String(r, encoding);
//							//Log.info("raw body:" +body);
//													
//						}
					

					if ((value.getBody().getClean_visible() != null))
					{
							
							body = value.getBody().getClean_visible().toString();
							//Log.info("clean_visible value "+ value.getBody().getClean_visible().toString());
					}

					
					// System.out.print("Body Body :" + body);
					// Get title and anchor
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


					String title_body_anchor = body + " " + title + " "
							+ anchor;
					title_body_anchor =title_body_anchor.replaceAll("\\s+"," ");
					Log.info("title_body_anchor"+ title_body_anchor);
					for (String topic : topicSet) {

						HashSet<String> canonical = new HashSet<String>();
						if (!topic.startsWith("https://twitter.com/")){
							Log.info("The topic at hand"+ topic);
							//canonical.add(topic.split("/")[4].replace("_", " "));
							canonical.add(topic.split("/")[4].replaceAll("_", " ").replace("(", "").replace(")", "").replaceAll("\\s+"," ").toLowerCase());
							//Log.info("hmmmmm" +canonical);
						}
						else{
							canonical.add(topic.split("/")[3].replaceAll("\\s+"," ").toLowerCase());
						}
						
						HashSet<String> canonical_partial = new HashSet<String>();
						if (!topic.startsWith("https://twitter.com/")){
							canonical_partial.addAll(Arrays.asList(topic.split("/")[4].replaceAll("_", " ").replace("(", "").replace(")", "").replaceAll("\\s+"," ").toLowerCase().split(" ")));
							//canonical_partial.addAll(listremove(Arrays.asList(topic.split("/")[4].replaceAll("_", " ").replace("(", "").replace(")", "").replaceAll("\\s+"," ").toLowerCase().split(" "))));
						}
						else {
							
							canonical_partial.addAll(Arrays.asList(topic.split("/")[3].replaceAll("\\s+"," ").toLowerCase().split(" ")));
							//canonical_partial.addAll(listremove(Arrays.asList(topic.split("/")[3].replaceAll("\\s+"," ").toLowerCase().split(" "))));
							
						}

						HashSet<String> label_name_redirect_canonical_partial =new HashSet<String>();
						
						try{
						for(String all_str: DbPedia.get(topic)){
							
							
							label_name_redirect_canonical_partial.addAll(Arrays.asList(all_str.replaceAll(",", " ").split(" ")));
							//label_name_redirect_canonical_partial.addAll(listremove(Arrays.asList(all_str.replaceAll(",", " ").split(" ")))); 
							
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
							Integer labelMentionCount =0;
							if (containsMention(title_body_anchor,
									label_name_redirect_canonical) == 1){
								 all =1;
							}
							if (containsMention(title_body_anchor,
									canonical_partial) == 1){
								 cano_part =1;
							}
							if (containsMention(title_body_anchor,
									canonical) == 1){
								 cano =1;
							}
							labelMentionCount = frequencyCounter(title_body_anchor,  label_name_redirect_canonical);

							
						
							
							//if(TrainTest.containsKey(streamid) && TrainTest.get(streamid).equalsIgnoreCase(key.toString())){ 
							
								
							context.write(new Text( topic),   new Text( all_part.toString()+"\t"+ all.toString()+"\t"+ cano_part.toString()+"\t"+ cano.toString()+"\t" +labelMentionCount.toString()));
							 //}

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
		
		
		private Map<String, HashSet<String>> loadLabels(String xcontext) {
			
			Map<String, HashSet<String>> prep = new LinkedHashMap<String, HashSet<String>>();
			Hashtable<String, HashSet<String>> tmp = new Hashtable<String, HashSet<String>>();
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

					temp_key = str[0];

					temp_value = str[1];

					
					HashSet<String> keys = (HashSet<String>) tmp.get(temp_key);
					if (keys == null) {
						keys = new HashSet<String>();
						tmp.put(temp_key, keys);
					}
					keys.add(temp_value.toLowerCase());
					
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

		public int frequencyCounter(String str, HashSet<String> hs) {
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
			return counter;
		}

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new KBANameVariantMatchTHERank(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ KBANameVariantMatchTHERank.class.getName()
						+ "-i input \n"
						+ "-o output \n "
						+ "-l labels file\n "
						+ "Example usage: hadoop jar cwi-trec/TRECKBA/TRECKBA2013-in_2014.jar nl.cwi.kba2013.apps.KbaNameVariantMatch "
						+ KBANameVariantMatchTHERank.class.getName()
						+ " "
						+ "-i KBA/Data/kba2013/kba2013-seq-cleansed-anno/*/* "
						+ "-o KBA/OutPut/kba2013-seq-cleansed-raw-with-short-count "
						+ "-l KBA/Data/kba2013/all-combined-wiki-and-twitte-and-cano.txt"
						);

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;
		
		String labelsFile = null;
		
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					in = args[++i];
				} else if ("-o".equals(args[i])) {
					out = args[++i];
				
				}  else if ("-l".equals(args[i])) {
					labelsFile = args[++i];
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
				|| labelsFile == null)
			return printUsage();

	
		LOG.info("Tool: " + this.getClass().getName());
		LOG.info(" - input path: " + in);
		LOG.info(" - output path: " + out);
		

		

		Configuration conf = getConf();
		
		conf.set(LABELSFILEPATH_HDFS, new Path(labelsFile).toUri().toString());
		
		
		// set time
		conf.setLong("mapred.task.timeout", 40 * 600000);
		conf.set("mapred.map.child.java.opts", "-Xmx4g -XX:-UseGCOverheadLimit");
		conf.set("mapred.child.java.opts", "-Xmx4096m");

		
		
		Job job = new Job(conf, "Feature Extractor");
		job.setJarByClass(KBANameVariantMatchTHERank.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.
		
		DistributedCache.addCacheFile(new URI(new Path(labelsFile) + "#"
				+ LABELSFILEPATH_HDFS), job.getConfiguration());
		
		DistributedCache.createSymlink(job.getConfiguration());
		
		
		job.setInputFormatClass(ThriftFileInputFormat.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setMapperClass(MyMapper.class);
	    job.setCombinerClass(MyReducer.class);
	    job.setReducerClass(MyReducer.class);
	    FileInputFormat.addInputPath(job, new Path(in));
	    job.setNumReduceTasks(50);

	    FileSystem.get(conf).delete(new Path(out), true);
	    TextOutputFormat.setOutputPath(job, new Path(out));
	    //job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    //job.setOutputValueClass(StreamItemWritable.class);
	    job.setOutputValueClass(Text.class);
	    //LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
	    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	   
	    int status = job.waitForCompletion(true) ? 0 : 1;

				

		return status;

	}
}
