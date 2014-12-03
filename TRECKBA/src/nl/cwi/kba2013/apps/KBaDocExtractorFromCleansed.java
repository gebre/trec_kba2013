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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import nl.cwi.kba2013.thrift.bin.StreamItemWritable;
import nl.cwi.kba2013.thrift.bin.ThriftFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



/**
 * TThis program extracts annotation documents  from either the training set or test set.
 * documents based on mere lexical matching.
 * 
 * @author Gebremeskel
 * 
 */
public class KBaDocExtractorFromCleansed extends Configured implements Tool {

	public static final String TrainTest_HDFS = "tt";

	public static Map<String, String> TrainTest = new LinkedHashMap<String, String>();

	

	protected enum Counter {
		documents, doc_with_body, body_raw, body_with_clean_html, body_with_clean_vissible
	};

	private static final Logger LOG = Logger
			.getLogger(KBaDocExtractorFromCleansed.class);

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		private static final Text out = new Text();
		private static final Text none = new Text("");

		@Override
		public void reduce(Text date, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			context.write(out, none);

		}
	}

	public static class MyMapper extends
			Mapper<Text, StreamItemWritable, Text, StreamItemWritable> {

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			TrainTest = loadTrainTest(TrainTest_HDFS);

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		public void map(Text key, StreamItemWritable value, Context context)
				throws IOException, InterruptedException {

			String streamid = value.getStream_id();

			String[] key_split = key.toString().split("/");

			if (TrainTest.containsKey(streamid)&& TrainTest.get(streamid).toString().equalsIgnoreCase(key_split[9])) { 

				context.write(new Text(key_split[9]), value);

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
				new KBaDocExtractorFromCleansed(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ KBaDocExtractorFromCleansed.class.getName()
						+ " -i input \n"
						+ "-o output\n "
						+ "tt train-teststream-id-hour) \n"

						+ "Example usage: hadoop jar trec-kba.jar "
						+ KBaDocExtractorFromCleansed.class.getName()
						+ " "
						+ "-i /data/private/trec_kba/kba-streamcorpus-2013/*/* "
						+ "-o KBA/OutPut/kba2013-seq-cleansed-anno"
						+ "-t KBA/Data/kba2013/train-test-2013-system_id-hour2.txt");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;
		String traintestFile = null;
		

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					in = args[++i];
				} else if ("-o".equals(args[i])) {
					out = args[++i];
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

		

		Configuration conf = getConf();
		conf.set(TrainTest_HDFS, new Path(traintestFile).toUri().toString());

		// set time
		conf.setLong("mapred.task.timeout", 40 * 600000);
		conf.set("mapred.map.child.java.opts", "-Xmx4g -XX:-UseGCOverheadLimit");
		conf.set("mapred.child.java.opts", "-Xmx4096m");

		Job job = new Job(conf, "Annotation Extraction");
		job.setJarByClass(KBaDocExtractorFromCleansed.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		job.setUserClassesTakesPrecedence(true);

		DistributedCache.addCacheFile(new URI(new Path(traintestFile) + "#"
				+ TrainTest_HDFS), job.getConfiguration());

		DistributedCache.createSymlink(job.getConfiguration());

		job.setInputFormatClass(ThriftFileInputFormat.class);

		job.setMapperClass(MyMapper.class);
		FileInputFormat.addInputPath(job, new Path(in));

		job.setNumReduceTasks(0);

		FileSystem.get(conf).delete(new Path(out), true);
		SequenceFileOutputFormat.setOutputPath(job, new Path(out));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StreamItemWritable.class);
		LazyOutputFormat.setOutputFormatClass(job,
				SequenceFileOutputFormat.class);

		int status = job.waitForCompletion(true) ? 0 : 1;

		Counters c = job.getCounters();

		long cputime = c.findCounter(
				org.apache.hadoop.mapred.Task.Counter.CPU_MILLISECONDS)
				.getValue();
		

		return status;

	}

}
