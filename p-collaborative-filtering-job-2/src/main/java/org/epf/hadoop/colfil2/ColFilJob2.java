package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {
    public static void main(String[] args) throws Exception {
        // Vérification des arguments
        if (args.length < 2) {
            System.err.println("Usage: ColFilJob2 <input path> <output path>");
            System.exit(-1);
        }

        // Configuration Hadoop
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering Job 2");

        // Définir la classe principale
        job.setJarByClass(ColFilJob2.class);

        // Définir les classes Mapper et Reducer
        job.setMapperClass(CommonRelationMapper.class);
        job.setReducerClass(CommonRelationReducer.class);

        // Définir les types de sortie du Mapper
        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Définir les types de sortie finale
        job.setOutputKeyClass(UserPair.class);
        job.setOutputValueClass(IntWritable.class);

        // Définir les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0])); // Chemin d'entrée
        Path outputPath = new Path(args[1]); // Chemin de sortie

        // Vérifier et supprimer le répertoire de sortie s'il existe
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        // Lancer le job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
