package lab2; 

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class AADriver extends Configured implements Tool {
    
   public int run(String[] args) throws Exception {

      // configure  MR job 
      Job job;
      job = new Job(getConf(), "amino-acid-counter" + System.getProperty("user.name"));
      job.setJarByClass(AADriver.class);
      job.setMapperClass(AAMapper.class);
      job.setReducerClass(AAReducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputValueClass(Text.class);

      // setup input and output paths for  MR job
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      // configure the distributed cache for codon mapping file
      DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());

      // run  MR job syncronously with verbose output set to true
      boolean returnCode = job.waitForCompletion(true);      
      return returnCode ? 0 : 1;
   }

   public static void main(String[] args) throws Exception {
       if(args.length != 3) {
           System.err.println("usage: AAdriver <input-file> <output-dir> <codon-table>");
           System.exit(1);
       }
       Configuration conf = new Configuration();        
       System.exit(ToolRunner.run(conf, new AADriver(), args));
   } 
}