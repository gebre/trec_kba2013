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

package nl.cwi.kba.apps;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.cwi.json.topics.Filter_topics;
import nl.cwi.json2012.run.Filter_run;
import nl.cwi.kba.thrift.bin.StreamItemWritable;
import nl.cwi.kba.thrift.bin.ThriftFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * TThis program extracts features from either the training set or test set.
 * documents based on mere lexical matching.
 * 
 * @author G.G Gebremeskel
 * 
 */
public class FeatureExtractor_filterer extends Configured implements Tool {

	public static final String QUERYFILEPATH_HDFS = "kba.topicfilelocation";
	public static final String LABELSFILEPATH_HDFS = "kba.labels";
	public static final String ANNOFILEPATH_HDFS = "kba.anno";
	
	public static  String anno_file ="";
	public static Map<String, String> all_attr = new LinkedHashMap<String, String>();
	public static Map<String, HashSet<String>> DbPedia = new LinkedHashMap<String, HashSet<String>>();
	public static Map<String, Integer> Annotation = new LinkedHashMap<String, Integer>();
	
	
	// public static Map<String, String> Attr ;

	public static final String RUNTAG = "kba.runtag";
	public static final String TEAMNAME = "kba.teamname";

	protected enum Counter {
		documents
	};

	private static final Logger LOG = Logger
			.getLogger(FeatureExtractor_filterer.class);

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
	 * @author emeij
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
			 anno_file = new
			 Path(context.getConfiguration().get(ANNOFILEPATH_HDFS)).toString();
			
			DbPedia = loadLabels(LABELSFILEPATH_HDFS);
			Annotation = loadAnnotation(ANNOFILEPATH_HDFS);
			
		
			
			System.out.println("What are the strings:\n");
			for(String k: DbPedia.keySet())
				System.out.println(k + "\t"+ DbPedia.get(k).size() );
			System.out.println("Annotations size :"
					+ Annotation.keySet().size());
			System.out.println("DbPEdia Keys :" + DbPedia.keySet().size());

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
		
			all_attr.put("LengthTitle", "0");
			all_attr.put("LengthBody", "0");
			all_attr.put("LengthAnchor", "0");
			all_attr.put("Source", "0");
			all_attr.put("English", "0");
			all_attr.put("MentionsTitle", "0");
			all_attr.put("MentionsBody", "0");
			all_attr.put("MentionsAnchor", "0");
			all_attr.put("FirstPos", "0");
			all_attr.put("LastPos", "0");
			all_attr.put("Spread", "0");
			all_attr.put("FirstPosNorm", "0");
			all_attr.put("LastPosNorm", "0");
			all_attr.put("SpreadNorm", "0");
			// all_attr.put("Related", "0");
			all_attr.put("Relatedtitle", "0");
			all_attr.put("RelatedBody", "0");
			all_attr.put("RelatedAnchor", "0");
			

			

			String body = new String(value.getBody().getCleansed());
			String title = new String(value.getTitle().getRaw().toString());
			String anchor = new String(value.getAnchor().getRaw().toString());

			String streamid = value.getStream_id();
			
			Integer lengthTitle = new StringTokenizer(title).countTokens();
			Integer lengthBody = new StringTokenizer(body).countTokens();
			Integer lengthAnchor = new StringTokenizer(anchor).countTokens();

			all_attr.put("LengthTitle", lengthTitle.toString());
			all_attr.put("LengthBody", lengthBody.toString());
			all_attr.put("LengthAnchor", lengthAnchor.toString());

			String sourcevalue = new String(value.getSource());
			Integer source = 0;
			if (sourcevalue == "news") {
				source = 0;
			} else if (sourcevalue == "social") {
				source = 1;
			} else if (sourcevalue == "linking") {
				source = 2;

			}
			all_attr.put("Source", source.toString());

			
			all_attr.put("Relatedtitle", entityMentionCounter(title));
			all_attr.put("RelatedBody", entityMentionCounter(body));
			all_attr.put("RelatedAnchor", entityMentionCounter(anchor));
			
			for (String topic : topicregexes.keySet()) {
				// int relde=0;
				// long count = 0;
				String stream_id_doc_id = streamid + "\t" + topic;
				stream_doc_id.set(streamid + "_" + topic);
				Integer anno_value = 0;

			
				if (Annotation.containsKey(stream_doc_id.toString())) {
					
					
					if (Annotation.get(stream_doc_id.toString()).intValue() == 2) {
						anno_value = 1; // relevant

					} else if (Annotation.get(stream_doc_id.toString())
							.intValue() == 1) {
						anno_value = -3; // Set it to -3 to show that it is

					}

					all_attr.put("Class", anno_value.toString());
					
					all_attr.put("Class", "?");

					if (containsMention(body, DbPedia.get(topic)) == 1) {

						
						
						Integer firstPos = returnFirstIndex(body,
								DbPedia.get(topic));
						Integer lastPos = returnLastIndex(body,
								DbPedia.get(topic));

						Integer spread = lastPos - firstPos;
						Integer firstPosNorm = firstPos.intValue()
								/ lengthBody.intValue();
						Integer lastPosNorm = lastPos.intValue()
								/ lengthBody.intValue();
						Integer spreadNorm = spread / lengthBody.intValue();
						all_attr.put("FirstPos", firstPos.toString());
						all_attr.put("LastPos", lastPos.toString());
						all_attr.put("Spread", spread.toString());
						all_attr.put("FirstPosNorm", firstPosNorm.toString());
						all_attr.put("LastPosNorm", lastPosNorm.toString());
						all_attr.put("SpreadNorm", spreadNorm.toString());
						all_attr.put("MentionsTitle",
								frequencyCounter(title, DbPedia.get(topic)));
						all_attr.put("MentionsBody",
								frequencyCounter(body, DbPedia.get(topic)));

						all_attr.put("MentionsAnchor",
								frequencyCounter(anchor, DbPedia.get(topic)));

						String all_values = "";
						for (String key1 : all_attr.keySet()) {
							if (all_values.equalsIgnoreCase("")) {
								all_values = all_attr.get(key1);
							} else {
								all_values = all_values + ","
										+ all_attr.get(key1);
							}
						}
														 
						 if(anno_file.endsWith("KBA/Data/trec-kba-ccr-2012-judgments-2012JUN22-final.before-cutoff.filter-run.txt")){
							 all_attr.put("Class", "?"); 
							 anno_value =0;
						 }
						if (anno_value != -3)
							context.write(new Text("CWI" + "\t" + "toy_j48"
									+ "\t" + stream_id_doc_id + "\t"),
									new Text(all_values));

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
				Pattern pattern = Pattern.compile(entity);
				Matcher matcher = pattern.matcher(str);

				while (matcher.find())
					counter++;
			}
			return counter.toString();
		}

		public String entityMentionCounter(String str) {
			Integer rel = 0;
			for (String topic : topicregexes.keySet()) {
				if (containsMention(str, DbPedia.get(topic)) == 1) {
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

				LOG.info(ft.getTopic_set_id());

				for (String t : ft.getTopic_names()) {

					Pattern p;

					// add the full name
					p = Pattern.compile(".*\\b+" + t.replaceAll("_", " ")
							+ "\\b+.*", Pattern.CASE_INSENSITIVE);
					topicregexes.put(t, p);

					// add the individual terms
					HashSet<String> pset = new HashSet<String>();
					pset.addAll(new HashSet<String>(Arrays.asList(t.split("_"))));
					pset.add(t.replaceAll("_", " "));
					partialtopics.put(t, pset);

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

		private Map loadLabels(String xcontext) {
			// Hashtable<String, Hashtable> prep = new Hashtable();
			Map<String, HashSet<String>> prep = new LinkedHashMap<String, HashSet<String>>();
			Hashtable<String, HashSet> tmp = new Hashtable();
			DataInputStream in = null;

			// System.out.println("This is from LoadLabels");
			System.out.println("\nwhat am I opening :" + xcontext);
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
					//keys.add(temp_value.toLowerCase());
					 keys.add(temp_value);
				}
				for (Object entity : tmp.keySet()) {
					prep.put((String) entity, tmp.get(entity));
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
			return prep;
		}

		private Map loadAnnotation(String xcontext) {
			Map<String, Integer> prep = new LinkedHashMap<String, Integer>();
			Hashtable<String, HashSet> tmp = new Hashtable();
			DataInputStream in = null;

			System.out.println("\nwhat am I opening :" + xcontext);
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


	}

	/**
	 * Loads the JSON topic file.
	 * 
	 * @param context
	 */

	private static void loadTopicData(String queryfile, Filter_run fr,
			FileSystem fs, HashMap<String, Object> run_info) {

		FSDataInputStream in = null;
		try {

			in = fs.open(new Path(queryfile));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			Filter_topics ft = new Filter_topics.Factory().loadTopics(br);

			fr.setTopic_set_id(ft.getTopic_set_id());
			run_info.put("num_entities", ft.getTopic_names().size());

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
				new FeatureExtractor_filterer(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ FeatureExtractor_filterer.class.getName()
						+ " -i input -o output -q query_file (hdfs) [-c corpus_id -r runtag -t teamname -d description] \n\n"
						+ "Example usage: hadoop jar trec-kba.jar "
						+ FeatureExtractor_filterer.class.getName()
						+ " "
						+ "-i kba/tiny-kba-stream-corpus/*/* "
						+ "-o kba/tiny-kba-stream-corpus-out "
						+ "-q kba/filter-topics.sample-trec-kba-targets-2012.json \n\n");
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
		String corpus_id = null;
		String runtag = null;
		String teamname = null;
		String annoFile = null;
		String gcldFile = null;
		String labelsFile = null;
		String pprFile = null;
		String myverFile = null;
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

		LOG.info("Tool: " + this.getClass().getName());
		LOG.info(" - input path: " + in);
		LOG.info(" - output path: " + out);
		LOG.info(" - runtag: " + runtag);
		LOG.info(" - teamname: " + teamname);
		LOG.info(" - corpus_id: " + corpus_id);
		LOG.info(" - run description: " + systemdescription);

		Filter_run fr = new Filter_run.Factory().create(TEAMNAME, RUNTAG,
				systemdescription, corpus_id);

		Map<String, String> Attr = new LinkedHashMap<String, String>();
		// Attr.put("trec-kba", "");
		Attr.put("LengthTitle", "");
		Attr.put("LengthBody", "");
		Attr.put("LengthAnchor", "");
		Attr.put("Source", "");
		Attr.put("English", "");
		Attr.put("MentionsTitle", "");
		Attr.put("MentionsBody", "");
		Attr.put("MentionsAnchor", "");
		Attr.put("FirstPos", "");
		Attr.put("LastPos", "");
		Attr.put("Spread", "");
		Attr.put("FirstPosNorm", "");
		Attr.put("LastPosNorm", "");
		Attr.put("SpreadNorm", "");
		// Attr.put("Related", "");
		Attr.put("Relatedtitle", "");
		Attr.put("RelatedBody", "");
		Attr.put("RelatedAnchor", "");
		
		//Attr.put("contxL", "0");
		//Attr.put("contxR", "0");
		Attr.put("Class", "");

		Configuration conf = getConf();
		conf.set(QUERYFILEPATH_HDFS, new Path(queryfile).toUri().toString());
		conf.set(LABELSFILEPATH_HDFS, new Path(labelsFile).toUri().toString());
		conf.set(ANNOFILEPATH_HDFS, new Path(annoFile).toUri().toString());
		
		conf.set(RUNTAG, runtag);
		conf.set(TEAMNAME, teamname);
		
		//set time
		conf.setLong("mapred.task.timeout", 40*600000);

		FileSystem fs = FileSystem.get(conf);
		// Lookup required data from the topic file
		loadTopicData(queryfile, fr, fs, run_info);
		Job job = new Job(conf, "Toy KBA system");
		job.setJarByClass(FeatureExtractor_filterer.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.
		DistributedCache.addCacheFile(new URI(new Path(queryfile) + "#"
				+ QUERYFILEPATH_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(labelsFile) + "#"
				+ LABELSFILEPATH_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(annoFile) + "#"
				+ ANNOFILEPATH_HDFS), job.getConfiguration());
		
		DistributedCache.createSymlink(job.getConfiguration());

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

		Counters c = job.getCounters();
		long cputime = c.findCounter(
				org.apache.hadoop.mapred.Task.Counter.CPU_MILLISECONDS)
				.getValue();
		run_info.put("elapsed_time_secs", ((double) cputime / 1000d));

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

		// System.out.println("#" + new Filter_run.Factory().toJSON(fr));
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
		Text line = new Text();
		LineReader reader = new LineReader(fs.open(new Path(out
				+ "/part-r-00000")));
		for (int i = 0; i < num_filter_results; i++) {
			reader.readLine(line);
			System.out.println(line.toString().split("\t\t")[1]);
		}
		

		return status;

	}
}
