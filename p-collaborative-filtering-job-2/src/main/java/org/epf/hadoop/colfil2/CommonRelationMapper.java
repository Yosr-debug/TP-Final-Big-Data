package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class CommonRelationMapper extends Mapper<LongWritable, Text, UserPair, Text> {
    private UserPair userPair = new UserPair();
    private Text relationFlag = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        if (tokens.length != 2) return;

        String user = tokens[0];
        String[] relations = tokens[1].split(",");

        HashSet<String> relationSet = new HashSet<>(Arrays.asList(relations));

        for (String friend1 : relations) {
            for (String friend2 : relations) {
                if (!friend1.equals(friend2)) {
                    userPair = new UserPair(friend1, friend2);
                    if (relationSet.contains(friend2)) {
                        relationFlag.set("0"); // Ils sont déjà amis
                    } else {
                        relationFlag.set("1"); // Relation commune trouvée
                    }
                    context.write(userPair, relationFlag);
                }
            }
        }
    }
}
