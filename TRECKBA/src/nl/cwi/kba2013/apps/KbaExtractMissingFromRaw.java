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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
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
public class KbaExtractMissingFromRaw extends Configured implements Tool {

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
			.getLogger(KbaExtractMissingFromRaw.class);

	
	public final class MyReducer extends Reducer<Text, Text, Text, Text>
	{
	 private MultipleOutputs<Text, Text> mos;

	 @Override
	 public void setup(final Context context)
	 {
	  mos = new MultipleOutputs<Text, Text>(context);
	 }

	 @Override
	 public void cleanup(final Context context) throws IOException, InterruptedException
	 {
	  mos.close();
	 }

	 @Override
	 public void reduce(final Text key, final Iterable<Text> values, final Context context)
	   throws IOException, InterruptedException
	 {
		 for (Text value : values) {

	  mos.write(key, value, "hello");
	  context.progress();
		 }
	 }
	}
	
	/**
	 * Emits date, PairOfStringLong pairs, where the string contains the doc-no
	 * and the topic and the long contains the score.
	 * 
	 * 
	 */
	public static class MyMapper extends
			Mapper<Text, StreamItemWritable, Text, Text> {

		private MultipleOutputs<Text, Text> mos;
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			
			
			mos = new MultipleOutputs<Text, Text>(context);

			DbPedia = loadLabels(LABELSFILEPATH_HDFS);

			TrainTest = loadTrainTest(TrainTest_HDFS);
			System.out.println("How many docids " + DbPedia.keySet().size());
			for (String k:DbPedia.keySet()){
				
			String[] array = DbPedia.get(k).toArray(new String[0]);
				System.out.println(k+":"+Arrays.toString(array));
			}

			

		}

		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
			mos.close();
		}

		@Override
		public void map(Text key, StreamItemWritable value, Context context)
				throws IOException, InterruptedException {

			
			
			context.getCounter(Counter.documents).increment(1);


			//Let check if editing works 
			//Log.info("watch streamid "+key.toString().split("/")[7]);
			if (value.getBody() != null) {
			
				ContentItem b =value.getBody();
				String encoding;
				String abs_url;
				String org_url;
				String schost=null;
				if (b.getEncoding()!=null){
					encoding=b.getEncoding();
				}
				else{
					encoding="UTF-8";
				}
				
				if (value.getAbs_url()!=null){
					abs_url=new String(value.getAbs_url(), encoding);
				}
				else{
					abs_url="NULL";
				}
				
				if (value.getOriginal_url()!=null){
					org_url=new String(value.getOriginal_url(), encoding);
				}
				
				else{
					org_url="NULL";
				}
				
				if (value.getSchost()!=null){
					schost=value.getSchost();
				}
				else{
					org_url="NULL";
				}
				
				if (value.getBody().getRaw()!=null){
					
					//if (value.getBody().getClean_visible() != null){
					String body = new String () ;
						//raw
						body = new String(value.getBody().getRaw(), encoding);
						
												//cleansed
						//body = value.getBody().getClean_visible().toString();
						
					
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
					//String url = new String();
					String source = new String();
					String doc_ent;

					if(value.getSource()!=null){
						source = value.getSource();	
					}
					
					
					String streamid = value.getStream_id();


					 	String[] key_split = key.toString().split("/");
						
							Log.info("see key" + key);
							if(TrainTest.containsKey(streamid)){ 
								Log.info("why so " + TrainTest.get(streamid) );
							String [] sp=TrainTest.get(streamid).toString().split("/");
							if (TrainTest.get(streamid).startsWith("https://twitter")){
								doc_ent=streamid +"_"+sp[3];
							}
							else{
								doc_ent=streamid +"_"+sp[4];
							}
							Log.info("why so " + TrainTest.get(streamid) );
								//context.write(new Text(streamid +"\t"+  TrainTest.get(streamid).toString() +"\t"+source+"\t"+ DbPedia.get(TrainTest.get(streamid).toString())+"\t"+ title + "\t" +anchor),
										//new Text(" ===\n"+body));
								mos.write(new Text(streamid +"\t"+  TrainTest.get(streamid).toString() +"\t"+source+"\t"+ DbPedia.get(TrainTest.get(streamid).toString())+"\t"+ title + "\t" +anchor+"\t"+abs_url+"\t"+org_url+"\t"+schost),
										new Text(" ===\n"+body), doc_ent);
							 }

						
					

				}
			}
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

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new KbaExtractMissingFromRaw(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ KbaExtractMissingFromRaw.class.getName()
						+ "-i input \n"
						+ "-o output \n "
						+ "-q query_file (hdfs) \n"
						+ "-l labels file\n "
						+ "-t train-teststream-id-hour file \n"
						+ "Example usage: hadoop jar cwi-trec/TRECKBA/TRECKBA2013-in_2014.jar  "
						+ KbaExtractMissingFromRaw.class.getName()
						+ " "
						+ "-i KBA/Data/kba2013/kba2013-seq-cleansed-anno/*/* "
						+ "-o KBA/OutPut/kba2013-seq-cleansed-raw-with-short-count "
						+ "-q KBA/Data/kba2013/trec-kba-ccr-and-ssf-query-topics-2013-04-08.json "
						+ "-l KBA/Data/kba2013/trec-kba-topics-labels.final3.txt "
						+ "-t KBA/Data/kba2013/train-test-2013-system_id-hour2.txt"
						);

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;
		String queryfile = null;
		
		String labelsFile = null;
		
		String traintestFile = null;
		

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					in = args[++i];
				} else if ("-o".equals(args[i])) {
					out = args[++i];
				} else if ("-q".equals(args[i])) {
					queryfile = args[++i];
				}  else if ("-l".equals(args[i])) {
					labelsFile = args[++i];
				} else if ("-t".equals(args[i])) {
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

	
		LOG.info("Tool: " + this.getClass().getName());
		LOG.info(" - input path: " + in);
		LOG.info(" - output path: " + out);
		

		

		Configuration conf = getConf();
		conf.set(QUERYFILEPATH_HDFS, new Path(queryfile).toUri().toString());
		conf.set(LABELSFILEPATH_HDFS, new Path(labelsFile).toUri().toString());
		
		conf.set(TrainTest_HDFS, new Path(traintestFile).toUri().toString());
		

		// set time
		conf.setLong("mapred.task.timeout", 40 * 600000);
		conf.set("mapred.map.child.java.opts", "-Xmx4g -XX:-UseGCOverheadLimit");
		conf.set("mapred.child.java.opts", "-Xmx4096m");

		
		
		Job job = new Job(conf, "Missing Extractor");
		job.setJarByClass(KbaExtractMissingFromRaw.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.
		DistributedCache.addCacheFile(new URI(new Path(queryfile) + "#"
				+ QUERYFILEPATH_HDFS), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(new Path(labelsFile) + "#"
				+ LABELSFILEPATH_HDFS), job.getConfiguration());
		
		DistributedCache.addCacheFile(new URI(new Path(traintestFile) + "#"
				+ TrainTest_HDFS), job.getConfiguration());
		
		DistributedCache.createSymlink(job.getConfiguration());
		//let's see it crushing again
		
		//job.setInputFormatClass(ThriftFileInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setMapperClass(MyMapper.class);
	    //job.setReducerClass(MyReducer.class);
	    FileInputFormat.addInputPath(job, new Path(in));
	    job.setNumReduceTasks(0);

	    FileSystem.get(conf).delete(new Path(out), true);
	    SequenceFileOutputFormat.setOutputPath(job, new Path(out));
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
