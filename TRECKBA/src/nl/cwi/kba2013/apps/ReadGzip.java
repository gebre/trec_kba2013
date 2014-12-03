/*******************************************************************************
 * Copyright 2013 G.G Gebremeskel
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import nl.cwi.kba2013.thrift.bin.StreamItemWritable;
import nl.cwi.kba2013.thrift.bin.ThriftFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * TThis program extracts features from either the training set or test set.
 * documents based on mere lexical matching.
 * 
 * @author Gebremeskel
 * 
 */
public class ReadGzip extends Configured implements Tool {

	protected enum Counter {
		documents
	};

	private static final Logger LOG = Logger.getLogger(ReadGzip.class);

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		private static final Text out = new Text();
		private static final Text none = new Text("");

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

		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		public void map(Text key, StreamItemWritable value, Context context)
				throws IOException, InterruptedException {

			context.getCounter(Counter.documents).increment(1);
			
			///////Get the folder
			//Path filePath = ((FileSplit) context.getInputSplit()).getPath();
			

			//String body = new String(value.getBody().getClean_visible());

			String streamid = value.getStream_id();
			String[] key_split = key.toString().split("/");
			context.write(new Text(key_split[key_split.length-2]), new Text());

		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ReadGzip(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ ReadGzip.class.getName()
						+ " -i input \n"
						+ "-o output\n "

						+ ReadGzip.class.getName()
						+ " "
						+ "-i KBA/Data/kba2013/2013-02-13-21/*"
						+ "-o KBA/OutPut/kba2013/explor2 ");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;
		
		HashMap<String, Object> run_info = new HashMap<String, Object>();

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					in = args[++i];
				} else if ("-o".equals(args[i])) {
					out = args[++i];
				
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
		

		// set time
		conf.setLong("mapred.task.timeout", 40 * 600000);

		FileSystem fs = FileSystem.get(conf);
		// Lookup required data from the topic file
		
		Job job = new Job(conf, "Feature Extractor");
		job.setJarByClass(ReadGzip.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.
		
		
		 
		job.setInputFormatClass(ThriftFileInputFormat.class);
		job.setMapperClass(MyMapper.class);
		FileInputFormat.addInputPath(job, new Path(in));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// job.setCombinerClass(MyReducer.class);
		// job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(0);

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


		return status;

	}
}
