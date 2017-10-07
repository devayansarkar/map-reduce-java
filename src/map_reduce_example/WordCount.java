package map_reduce_example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;


public class WordCount {
		@SuppressWarnings("deprecation")
		public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
		{

				Configuration conf= new Configuration();
				Job job = new Job(conf,"WordCount");
				job.setJarByClass(WordCount.class);
				job.setMapperClass(MapData.class);
				job.setReducerClass(ReduceData.class);
				job.setOutputKeyClass(Text.class);

				job.setOutputValueClass(IntWritable.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				Path intputPath = new Path(args[0]);
				Path outputPath = new Path(args[1]);
				FileInputFormat.addInputPath(job, intputPath);
				FileOutputFormat.setOutputPath(job, outputPath);
				
				
				outputPath.getFileSystem(conf).delete(outputPath);
				System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
