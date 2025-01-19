package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonRelationReducer extends Reducer<UserPair, Text, UserPair, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        boolean alreadyFriends = false;

        for (Text value : values) {
            if (value.toString().equals("0")) {
                alreadyFriends = true;
            } else {
                count++;
            }
        }

        if (!alreadyFriends && count > 0) {
            result.set(String.valueOf(count));
            context.write(key, result);
        }
    }
}
