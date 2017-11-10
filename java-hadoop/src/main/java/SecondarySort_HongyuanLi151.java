
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort_HongyuanLi151 {

    public static class UriMapper_HongyuanLi151 extends Mapper<Object, Text, Pair_HongyuanLi151, NullWritable> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    String s = value.toString();
	    String[] tokens = s.split("\\t");
	    int val = Integer.valueOf(tokens[1]);
	    context.write(new Pair_HongyuanLi151(tokens[0], val), NullWritable.get());
	}
    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "secondary sort");
	job.setJarByClass(SecondarySort_HongyuanLi151.class);
	job.setMapperClass(UriMapper_HongyuanLi151.class);
	job.setOutputKeyClass(Pair_HongyuanLi151.class);
	job.setOutputValueClass(NullWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
