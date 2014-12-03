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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import nl.cwi.kba2013.apps.FeatureExtractor_DocExtractor.MyMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

//import nl.cwi.json2012.run.Filter_run;

/**
 * TThis program extracts features from either the training set or test set.
 * documents based on mere lexical matching.
 * 
 * @author Gebremeskel
 * 
 */
public class chunk_stream_DocExtractor extends Configured implements Tool {

	public static final String TrainTest_HDFS = "tt";
	public static Map<String, String> TrainTest = new LinkedHashMap<String, String>();

	protected enum Counter {
		documents
	};

	private static final Logger LOG = Logger
			.getLogger(chunk_stream_DocExtractor.class);

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		private static final Text out = new Text();
		private static final Text none = new Text("");

		@Override
		public void reduce(Text date, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			context.write(out, none);

		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {


		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			TrainTest = loadTrainTest(TrainTest_HDFS);
			System.out.println("Check Size" + TrainTest.size());
			for (String ke : TrainTest.keySet()) {
				System.out.println(ke + " \t" + TrainTest.get(ke));
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			context.getCounter(Counter.documents).increment(1);
			String line =value.toString();
			String  folder_part =line.split(":")[0];
			Log.info("What the hell"+folder_part);
			
			String part2=line.replace(folder_part+":", "");
						
			HashSet <String> chunk_set = new HashSet <String>(); 
			chunk_set.addAll(Arrays.asList(part2.replace("|", "=").split("=")));
			String [] part2_splitted = folder_part.split("/");
			String folder = part2_splitted[3];
			String gpg=part2_splitted[4];
			
			for (String s:chunk_set){
				String stream = s.replace("(", "").split(",")[0];
				Log.info("What the hell"+stream);

			if (TrainTest.containsKey(stream) && TrainTest.get(stream).equalsIgnoreCase(folder)) {
				context.write(new Text (folder+"/"+gpg), new Text(stream));
				//context.write( new Text(stream), new Text (folder));
			}
			}

		}

		private Map<String, String> loadTrainTest(String xcontext) {

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
				new chunk_stream_DocExtractor(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out.println("Usage: "
				+ chunk_stream_DocExtractor.class.getName() + " -i input \n"
				+ "-o output\n "
				+ "-tt KBA/Data/trec-kba-ccr-topics-2012-wiki.txt\n\n");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;

		String traintestFile = null;
		HashMap<String, Object> run_info = new HashMap<String, Object>();

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					in = args[++i];
				} else if ("-o".equals(args[i])) {
					out = args[++i];

				} else if ("-tt".equals(args[i])) {
					traintestFile = args[++i];
					Log.info("TrainTest");
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

		

		Configuration conf = getConf();

		conf.set(TrainTest_HDFS, new Path(traintestFile).toUri().toString());

		// set time
		conf.setLong("mapred.task.timeout", 40 * 600000);
		conf.set("mapred.map.child.java.opts", "-Xmx4g -XX:-UseGCOverheadLimit");
		conf.set("mapred.child.java.opts", "-Xmx4096m");

		Job job = new Job(conf, "chunk -stream");
		job.setJarByClass(chunk_stream_DocExtractor.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.

		DistributedCache.addCacheFile(new URI(new Path(traintestFile) + "#"
				+ TrainTest_HDFS), job.getConfiguration());

		DistributedCache.createSymlink(job.getConfiguration());
		

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
	    FileInputFormat.addInputPath(job, new Path(in));
		

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileSystem.get(conf).delete(new Path(out), true);
		TextOutputFormat.setOutputPath(job, new Path(out));
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

		return status;

	}
}
