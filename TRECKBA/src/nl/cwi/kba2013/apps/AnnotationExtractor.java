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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
public class AnnotationExtractor extends Configured implements Tool {

	public static final String ANNOFILEPATH_HDFS = "kba.anno";
	// public static Path anno_file;
	public static Map<String, Integer> Annotation = new LinkedHashMap<String, Integer>();

	private static final Logger LOG = Logger
			.getLogger(AnnotationExtractor.class);

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
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private static final Text stream_doc_id = new Text();

		// private static final Text featurLine = new Text();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			// anno_file = new Path(context.getConfiguration().get(
			// ANNOFILEPATH_HDFS));
			// FeatureExtractor.MyMapper m =new FeatureExtractor.MyMapper();
			 Annotation= loadAnnotation(ANNOFILEPATH_HDFS);
			System.out.println("From Annotation =========================\n\n");
			System.out.println("Annotation size" + Annotation.size());
			for(String k: Annotation.keySet()){
				System.out
				.println(k);	
			}
			System.out
					.println(Annotation
							.get("1330549680-8ff985e89aa3026bdd0783964ff632b3_https://twitter.com/RonFunches"));
			System.out.println("From Annotation =========================\n\n");

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
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] val_sp = value.toString().split("\t");
			  stream_doc_id.set(new Text(val_sp[2] + "_" + val_sp[3]));
			//System.out.println("\nThe combo :" + stream_doc_id.toString());
			//System.out.println("\n\nThe value :" + value.toString());

			int anno_value = 0;

			if (Annotation.containsKey(stream_doc_id.toString())) {

				if (Annotation.get(stream_doc_id.toString()).intValue() == 2) {
					anno_value = 1; // relevant

				} else if (Annotation.get(stream_doc_id.toString()).intValue() == 1) {
					anno_value = -3;

				}

				//all_attr.put( "Class", new Integer(Annotation.get(
				// stream_doc_id.toString()).intValue()) .toString());

				Log.info("The key is:" + stream_doc_id.toString());
				context.write(stream_doc_id, new Text(val_sp[5]));

			}

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
			System.out.println("\nWhat am I opening" + xcontext);
			try {

				in = new DataInputStream(new FileInputStream(xcontext));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;

				// Read File Line By Line
				while ((strLine = br.readLine()) != null) {
					//System.out.println(" Annotation executed?");
					//System.out.println(strLine);
					String temp_key;
					String temp_value;

					String[] str = strLine.split("\t");

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
				LOG.error("did not read from distributed cache, sadly");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("File not found" + e);
			} finally {

				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
			System.out.println("So what is prep size" + prep.size());
			return prep;
		}

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new AnnotationExtractor(), args);
		System.exit(res);
	}

	static int printUsage() {
		System.out
				.println("Usage: "
						+ AnnotationExtractor.class.getName()
						+ " -i input \n"
						+ "-o output\n "
						+ "-a annotation file \n"

						+ "Example usage: hadoop jar trec-kba.jar "
						+ AnnotationExtractor.class.getName()
						+ " "
						+ " nl.cwi.kba2013.apps.AnnotationExtractor"
						+ "-i  KBA/OutPut/kba2013/feature2012_09_3/part-r-00000"
						+ "-o  KBA/OutPut/kba2013/Annotation"
						+ "-a KBA/Data/kba2013/trec-kba-ccr-judgments-2013-07-08.before-cutoff.filter-run.txt");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@Override
	public int run(String[] args) throws Exception {

		String in = null;
		String out = null;

		String annoFile = null;

	

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					in = args[++i];
				} else if ("-o".equals(args[i])) {
					out = args[++i];
				} else if ("-q".equals(args[i])) {

				} else if ("-a".equals(args[i])) {
					annoFile = args[++i];

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

		conf.set(ANNOFILEPATH_HDFS, new Path(annoFile).toUri().toString());

		// set time
		conf.setLong("mapred.task.timeout", 40 * 600000);
		conf.set("mapred.map.child.java.opts", "-Xmx4g -XX:-UseGCOverheadLimit");

		FileSystem fs = FileSystem.get(conf);
		// Lookup required data from the topic file

		Job job = new Job(conf, "Annotation Extractor");
		job.setJarByClass(AnnotationExtractor.class);

		// some weird issues with Thrift classes in the Hadoop distro.
		 //job.setUserClassesTakesPrecedence(true);

		// make the query file available to each mapper.

		DistributedCache.addCacheFile(new URI(new Path(annoFile) + "#"
				+ ANNOFILEPATH_HDFS), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());

		job.setInputFormatClass(TextInputFormat.class);
		//job.setMapperClass(MyMapper.class);
		FileInputFormat.addInputPath(job, new Path(in));

		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(MyReducer.class);
		//job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);

		FileSystem.get(conf).delete(new Path(out), true);
		TextOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Let's go
		int status = job.waitForCompletion(true) ? 0 : 1;

		return status;

	}
}
