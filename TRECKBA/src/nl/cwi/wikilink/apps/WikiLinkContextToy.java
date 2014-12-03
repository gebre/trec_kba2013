/*******************************************************************************
 * Copyright 2012 G.G Gebremeskel
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

package nl.cwi.wikilink.apps;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import nl.cwi.json.topics.Filter_topics;
import nl.cwi.wikilink.thrift.Mention;
import nl.cwi.wikilink.thrift.WikiLinkItem;
import nl.cwi.wikilink.thrift.bin.ThriftFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * This program tries to read a wikilink thrift file.
 * 
 * @author G.G Gebremeskel
 */
public class WikiLinkContextToy extends Configured implements Tool {

	public static final String RUNTAG = "wikilink";
	public static final String QUERYFILEPATH_HDFS = "kba.topicfilelocation";
	
	

	protected enum Counter {
		documents
	}

	private static final Logger LOG = Logger
			.getLogger(WikiLinkContextToy.class);

	public static class MyMapper extends
			Mapper<Text, WikiLinkItem, Text, Text> {
		private Map<String, Pattern> topicregexes = new LinkedHashMap<String, Pattern>();
		private Filter_topics ft = null;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);
			loadTopics(QUERYFILEPATH_HDFS);
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		public void map(Text key, WikiLinkItem value, Context context)
				throws IOException, InterruptedException {

			context.getCounter(Counter.documents).increment(1);

			
			Iterator it = value.getMentionsIterator();
			//String url = value.getUrl();
			
			/*System.out.println(url + " mentions the following \n");
			for (String k: topicregexes.keySet()){
				
			System.out.println( k);

		}
*/
			
			String filename = key.toString();

			// calculate the score as the relative frequency of occurring of the
			// entity in the document.
			// count = 1000 * (count * topic.length()) / body.length();
			//context.write(new Text( "========================================================"), new Text(""));
			//context.write(new Text(url+ ":"), new Text(""));
			//String collect ="";
			while(it.hasNext()){
				Mention m = (Mention) it.next();
				for(String k: topicregexes.keySet()){
					 String [] path_split=m.getWiki_url().toString().split("/");
					 String pageName =path_split[path_split.length-1];
					if(pageName.equalsIgnoreCase(k)){
						//context.write(new Text(k), new Text(m.toString()));
						
						nl.cwi.wikilink.thrift.Context con = m.getContext();
						if(con!=null) {						
						context.write(new Text(k+"\t"+con.getLeft().toString() + "\t"  + con.getRight().toString()), new Text(m.getAnchor_text().toString()));
					}

					}	
				}
				
			
			}
			
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
					/*
					HashSet<String> pset = new HashSet<String>();
					pset.addAll(new HashSet<String>(Arrays.asList(t.split("_"))));
					pset.add(t.replaceAll("_", " "));
					partialtopics.put(t, pset);
					*/

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
	}

	/**
	 * Loads the JSON topic file.
	 * 
	 * @param context
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new WikiLinkContextToy(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ WikiLinkContextToy.class.getName()
						+ " -i  input -o output   \n\n"
						+ "Example usage: hadoop jar TRECKBA_fat_new.jar nl.cwi.wikilink.apps.WikiLinkContextToy "
						+ WikiLinkContextToy.class.getName()
						+ " "
						+ "-i KBA/Data/wikilinks_trial/* "
						+ "-o KBA/OutPut/wikilinks "
						+ "-q KBA/Data/trec-kba-ccr-2012.filter-topics.json "
						);
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;
		String queryFile = null;
		

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					in = args[++i];
				} else if ("-o".equals(args[i])) {
					out = args[++i];
				} else if ("-q".equals(args[i])) {
					queryFile = args[++i];
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

		if (other_args.size() > 0 || in == null || out == null)
			return printUsage();

		
		LOG.info("Tool: " + this.getClass().getName());
		LOG.info(" - input path: " + in);
		LOG.info(" - output path: " + out);
		LOG.info(" - query file: " + queryFile);
		

		Configuration conf = getConf();
		conf.set(QUERYFILEPATH_HDFS, new Path(queryFile).toUri().toString());
		
		
		FileSystem fs = FileSystem.get(conf);
		// Lookup required data from the topic file
		// loadTopicData(queryfile, fr, fs, run_info);
		Job job = new Job(conf, "WikiLinks");
		job.setJarByClass(WikiLinkContextToy.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.
		
		DistributedCache.addCacheFile(new URI(new Path(queryFile) + "#"
				+ QUERYFILEPATH_HDFS), job.getConfiguration());
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

		// add some more statistics
		Counters c = job.getCounters();

		return status;

	}
}
