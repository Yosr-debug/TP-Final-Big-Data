package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob1 {
    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: ColFilJob1 <input path> <output path>");
            System.exit(-1);
        }


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering Job 1");


        job.setJarByClass(ColFilJob1.class);


        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));


        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
