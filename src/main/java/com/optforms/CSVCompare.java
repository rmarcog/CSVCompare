package com.optforms;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CSVCompare {

	public static void main(String[] args) {

		if (args.length != 5) {
			System.err
					.println("Incorrect arguments. Expected arguments: <xls file name 1> <xls file name 2> <column file1> <column file2> <output path>");
			return;
		}
		int col1 = 0;
		int col2 = 0;
		try {
			col1 = Integer.parseInt(args[2]);
			col2 = Integer.parseInt(args[3]);
		} catch (Exception e) {
			col1 = 0;
			col2 = 0;
		}
		col1--;
		col2--;
		if (col1 < 0 || col2 < 0) {
			System.err
					.println("Incorrect arguments. Expected arguments: <xls file name 1> <xls file name 2> <column file1> <column file2> <output path>");
			System.err
					.println("<column file1> and <column file2> must be an integer and greater than 0");
			return;
		}

		Configuration conf = new Configuration();
		conf.setInt("com.optforms.file1col", col1);
		conf.setInt("com.optforms.file2col", col2);

		Job job;
		try {
			job = new Job(conf, "xlscompare");
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		job.setJarByClass(CSVCompare.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, File1Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, File2Mapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			e.printStackTrace();
			return;
		}
	}

	public static class File1Mapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private final static String fileTag = "F1~";

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int colNum = context.getConfiguration().getInt(
					"com.optforms.file1col", 0);
			String[] cols = value.toString().split(
					"[,;](?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			if (colNum < cols.length) {
				String strkey = cols[colNum];
				// Remove quotes from string
				if (strkey.charAt(0) == '"'
						&& strkey.charAt(strkey.length() - 1) == '"') {
					strkey = strkey.substring(1, strkey.length() - 1);
				}
				context.write(new Text(strkey),
						new Text(fileTag + value.toString()));
			}
		}
	}

	public static class File2Mapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private final static String fileTag = "F2~";

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int colNum = context.getConfiguration().getInt(
					"com.optforms.file2col", 0);
			String[] cols = value.toString().split(
					"[,;](?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			if (colNum < cols.length) {
				String strkey = cols[colNum];
				// Remove quotes from string
				if (strkey.charAt(0) == '"'
						&& strkey.charAt(strkey.length() - 1) == '"') {
					strkey = strkey.substring(1, strkey.length() - 1);
				}
				context.write(new Text(strkey),
						new Text(fileTag + value.toString()));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private int File1Count = 0;
		private int File2Count = 0;
		private int Match = 0;

		private static final String File1Tag = "F1";
		private static final String File2Tag = "F2";

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Update file counters and check if line matches
			int sum = 0;
			String line = null;
			for (Text val : values) {
				String tags[] = val.toString().split("~");
				if (tags[0].equals(File1Tag)) {
					line = tags[1];
					File1Count++;
				}
				if (tags[0].equals(File2Tag))
					File2Count++;
				sum++;
			}
			if (sum == 2) {
				// Write whole line
				context.write(null, new Text(line));
				// Update match count
				Match++;
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Write count report
			if (File1Count == File2Count) {
				// Same number of rows in both files
				context.write(new Text("Match percent: "), new Text(
						Double.toString((Match * 100 / (double) File1Count))
								+ " (" + Match + " out of " + File1Count + ")"));
			} else {
				// Different number of rows
				context.write(new Text("File 1 match percent: "), new Text(
						Double.toString((Match * 100 / (double) File1Count))
								+ " (" + Match + " out of " + File1Count + ")"));
				context.write(new Text("File 2 match percent: "), new Text(
						Double.toString((Match * 100 / (double) File2Count))
								+ " (" + Match + " out of " + File2Count + ")"));
			}
		}
	}
}
