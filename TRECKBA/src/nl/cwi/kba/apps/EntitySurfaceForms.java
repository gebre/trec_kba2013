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
import java.util.regex.Pattern;

import nl.cwi.kba.thrift.bin.StreamItemWritable;
import nl.cwi.kba.thrift.bin.ThriftFileInputFormat;
import nl.cwi.json.topics.Filter_topics;
import nl.cwi.json2012.run.Filter_run;

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
 * Toy KBA system similar to the Python version. It identifies entities in
 * documents based on mere lexical matching.
 * 
 * @uthor Gebremeskel
 * 
 */
public class EntitySurfaceForms extends Configured implements Tool {

	public static final String QUERYFILEPATH_HDFS = "kba.topicfilelocation";
	public static final String LABELSFILEPATH_HDFS = "kba.labels";
	public static final String PPR_HDFS = "ppr";
	public static final String GCLD_HDFS = "gcld";

	public static Map<String, HashSet<String>> DbPedia = new LinkedHashMap<String, HashSet<String>>();
	public static Map<String, HashMap<String, Double>> Ppr = new LinkedHashMap<String, HashMap<String, Double>>();
	public static Map<String, HashMap<String, Double>> Gcld = new LinkedHashMap<String, HashMap<String, Double>>();

	// public static Map<String, String> Attr ;

	public static final String RUNTAG = "kba.runtag";
	public static final String TEAMNAME = "kba.teamname";

	protected enum Counter {
		documents
	};

	private static final Logger LOG = Logger
			.getLogger(EntitySurfaceForms.class);

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
		public static final int scale = 1000;
		private static final Text stream_doc_id = new Text();
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

			DbPedia = loadLabels(LABELSFILEPATH_HDFS);
			Ppr = loadPPR(PPR_HDFS);
			Gcld = loadGCLD(GCLD_HDFS);
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

			// partialtopics = DbPedia;

			String body = new String(value.getBody().getCleansed());
			String title = new String(value.getTitle().getRaw().toString());
			String anchor = new String(value.getAnchor().getRaw().toString());

			// lowercase all
			body = body.toLowerCase();
			title = title.toLowerCase();
			anchor = anchor.toLowerCase();

			String streamid = value.getStream_id();

			for (String topic : topicregexes.keySet()) {

				String stream_id_doc_id = streamid + "\t" + topic;

				Integer canonical_value = 0;
				Integer surfaceForm_value = 0;
				Integer ppr_value = 0;
				Integer gcld_value = 0;

				Double temp = 0.0;
				for (String partial : partialtopics.get(topic)) {
					if (containsMention(title + body + anchor,
							new HashSet<String>(Arrays.asList(partial))) == 1) {
						temp = temp + (double) partial.length()
								/ topic.length();
					}

				}
				temp = temp * scale;
				canonical_value = temp.intValue();
				temp = 0.0;

				for (String s_form : DbPedia.get(topic)) {
					if (containsMention(title + body + anchor,
							new HashSet<String>(Arrays.asList(s_form))) == 1) {
						temp = temp + (double) s_form.length() / topic.length();
					}

				}

				temp = temp * scale;
				surfaceForm_value = temp.intValue();
				temp = 0.0;

				for (String gcld_key : Gcld.get(topic).keySet()) {
					if (containsMention(title + body + anchor,
							new HashSet<String>(Arrays.asList(gcld_key))) == 1) {
						temp = temp + Gcld.get(topic).get(gcld_key);
					}

				}
				temp = temp * scale;
				gcld_value = temp.intValue();
				temp = 0.0;

				System.out.println("It is dying at " + topic);
				for (String ppr_key : Ppr.get(topic).keySet()) {
					if (containsMention(title + body + anchor,
							new HashSet<String>(Arrays.asList(ppr_key))) == 1) {
						temp = temp + Ppr.get(topic).get(ppr_key);
					}

				}
				temp = temp * scale;
				ppr_value = temp.intValue();
				
				if((canonical_value+surfaceForm_value+gcld_value+ppr_value) >0)
				context.write(new Text("CWI" + "\t" + "toy_j48" + "\t"
						+ stream_id_doc_id + "\t"),
						new Text(canonical_value.toString() + "\t"
								+ surfaceForm_value.toString() + "\t"
								+ ppr_value.toString() +"\t"+ gcld_value.toString()));

			}
		}

		public int containsMention(String str, HashSet<String> hs) {
			int i = 0;
			// System.out.println();
			for (String entity : hs) {
				if (str.contains(entity)) {
					i = 1;
				}
			}
			return i;
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
					pset.add(t.replaceAll("_", " ").toLowerCase());
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
			// System.out.println("\nDBpedia File :"+ LABELSFILEPATH_HDFS);

			// System.out.println("\nDBpedia File :"+LABELSFILEPATH_HDFS);
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

		private Map loadPPR(String xcontext) {
			// Hashtable<String, Hashtable> prep = new Hashtable();
			Map<String, HashMap<String, Double>> prep = new LinkedHashMap<String, HashMap<String, Double>>();
			Hashtable<String, HashSet> tmp = new Hashtable();
			DataInputStream in = null;
			// String xcontext
			// ="/ufs/gebre/Downloads/trec-kba-ccr-topics-2012.ppr-top100.txt";
			// System.out.println("This is from LoadLabels");
			// System.out.println("\nwhat am I opening :" + xcontext);
			// System.out.println("\nDBpedia File :"+ LABELSFILEPATH_HDFS);

			// System.out.println("\nPPr from loadPPR :"+xcontext);

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				// Read File Line By Line

				String key_check = "";
				HashMap<String, Double> intermediat = new HashMap<String, Double>();

				while ((strLine = br.readLine()) != null) {
					// Print the content on the console
					// System.out.println("\nFrom PPR strLin :"+strLine);
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

					temp_str = str[3];
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
						prep.put(key_check, intermediat);
						intermediat.clear();
						key_check = temp_key;
					}
					intermediat.put(temp_str.toLowerCase(), temp_value);

				}
				if (key_check.equalsIgnoreCase("William_H._Gates,_Sr.")) {
					key_check = "William_H._Gates,_Sr";
				}
				prep.put(key_check, intermediat);

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

		private Map loadGCLD(String xcontext) {
			// Hashtable<String, Hashtable> prep = new Hashtable();
			Map<String, HashMap<String, Double>> prep = new LinkedHashMap<String, HashMap<String, Double>>();
			Hashtable<String, HashSet> tmp = new Hashtable();
			DataInputStream in = null;
			// String xcontext
			// ="/ufs/gebre/Downloads/trec-kba-ccr-topics-2012.ppr-top100.txt";
			// System.out.println("This is from LoadLabels");
			// System.out.println("\nwhat am I opening :" + xcontext);
			// System.out.println("\nDBpedia File :"+ LABELSFILEPATH_HDFS);

			// System.out.println("\nDBpedia File :"+LABELSFILEPATH_HDFS);

			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				// Read File Line By Line

				String key_check = "";
				HashMap<String, Double> intermediat = new HashMap<String, Double>();

				while ((strLine = br.readLine()) != null) {
					// Print the content on the console
					String temp_key;
					Double temp_value;
					String temp_str;

					String[] str = new String(strLine.getBytes("UTF-8"),
							"UTF-8").split("\t");

					temp_str = str[0];
					temp_key = str[2];
					temp_value = Double.parseDouble(str[1])
							* Double.parseDouble(str[4]);
					if (key_check == "") {
						key_check = temp_key;
						// System.out.println( " Executed ! :) "+ key_check );
					}

					if (!key_check.equalsIgnoreCase(temp_key)) {
						// System.out.println(" When ! :) " + temp_key);
						prep.put(key_check, intermediat);
						intermediat.clear();
						key_check = temp_key;
					}
					intermediat.put(temp_str.toLowerCase(), temp_value);

				}
				prep.put(key_check, intermediat);

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
		int res = ToolRunner.run(new Configuration(), new EntitySurfaceForms(),
				args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ EntitySurfaceForms.class.getName()
						+ " -i input -o output -q query_file (hdfs) -l labels_file -p ppr_file -g -gcld_file [-c corpus_id -r runtag -t teamname -d description] \n\n"
						+ "Example usage: hadoop jar trec-kba.jar "
						+ EntitySurfaceForms.class.getName()
						+ " "
						+"-i KBA/Data/kba-stream-corpus-2012-cleansed-only-testing-out/*/* "
						+"-o KBA/OutPut/Kba_2012/cwi-cano_sform_ppr_gcld" 
						+" -q KBA/Data/trec-kba-ccr-2012.filter-topics.json"
						+"-l KBA/Data/entity-surface-forms_merged.txt"
						+"-p KBA/Data/trec-kba-ccr-topics-2012.ppr-top100.txt"
						+" -g KBA/Data/dictionary.txt  -r entity-sforms -t CWI -d \"scores doc-entity pair based on surface forms \""
						+">/export/scratch2/TREC-kba/TREC-KBA-2013/data/multi-step/Results/graphs/String-matching/cwi-cano_sform_ppr_gcld");

						
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;
		String queryfile = null;
		String systemdescription = null;
		String corpus_id = null;
		String runtag = null;
		String teamname = null;
		String gcldFile = null;
		String labelsFile = null;
		String pprFile = null;

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
				} else if ("-t".equals(args[i])) {
					teamname = args[++i];
				} else if ("-d".equals(args[i])) {
					systemdescription = args[++i];
				} else if ("-p".equals(args[i])) {
					pprFile = args[++i];
				} else if ("-g".equals(args[i])) {
					gcldFile = args[++i];
				} else if ("-c".equals(args[i])) {
					corpus_id = args[++i];
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

		Configuration conf = getConf();
		conf.set(QUERYFILEPATH_HDFS, new Path(queryfile).toUri().toString());
		conf.set(LABELSFILEPATH_HDFS, new Path(labelsFile).toUri().toString());
		conf.set(PPR_HDFS, new Path(pprFile).toUri().toString());
		conf.set(GCLD_HDFS, new Path(gcldFile).toUri().toString());
		conf.set(RUNTAG, runtag);
		conf.set(TEAMNAME, teamname);

		FileSystem fs = FileSystem.get(conf);
		// Lookup required data from the topic file
		loadTopicData(queryfile, fr, fs, run_info);
		Job job = new Job(conf, "Toy KBA system");
		job.setJarByClass(EntitySurfaceForms.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.
		DistributedCache.addCacheFile(new URI(new Path(queryfile) + "#"
				+ QUERYFILEPATH_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(labelsFile) + "#"
				+ LABELSFILEPATH_HDFS), job.getConfiguration());

		DistributedCache.addCacheFile(new URI(new Path(pprFile) + "#"
				+ PPR_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(gcldFile) + "#"
				+ GCLD_HDFS), job.getConfiguration());

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

		System.out.println("#" + new Filter_run.Factory().toJSON(fr));

		Text line = new Text();
		LineReader reader = new LineReader(fs.open(new Path(out
				+ "/part-r-00000")));
		for (int i = 0; i < num_filter_results; i++) {
			reader.readLine(line);
			System.out.println(line.toString());
		}

		System.out.println("#"
				+ new Filter_run.Factory().toPrettyJSON(fr).replaceAll("\\n",
						"\n#"));

		return status;

	}
}
