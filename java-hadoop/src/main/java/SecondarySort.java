
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort {

    public static class UriMapper extends Mapper<Object, Text, Pair, NullWritable> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    String s = value.toString();
	    String[] tokens = s.split("\\t");
	    int val = Integer.valueOf(tokens[1]);
	    context.write(new Pair(tokens[0], val), NullWritable.get());
	}
    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "secondary sort");
	job.setJarByClass(SecondarySort.class);
	job.setMapperClass(UriMapper.class);
	job.setOutputKeyClass(Pair.class);
	job.setOutputValueClass(NullWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
