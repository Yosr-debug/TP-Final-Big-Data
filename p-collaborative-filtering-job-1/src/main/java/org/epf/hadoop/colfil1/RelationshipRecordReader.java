package org.epf.hadoop.colfil1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.epf.hadoop.colfil1.Relationship;


import java.io.IOException;


public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable currentKey = new LongWritable();
    private Relationship currentValue = new Relationship();


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            return false;
        }

        currentKey.set(lineRecordReader.getCurrentKey().get());

        // Traitement de la ligne pour extraire id1 et id2
        String line = lineRecordReader.getCurrentValue().toString();
        String[] tokens = line.split("<->");
        if (tokens.length < 2) {
            throw new IOException("Invalid input format: " + line);
        }

        String[] user2AndTimestamp = tokens[1].split(",");
        currentValue.setId1(tokens[0].trim());
        currentValue.setId2(user2AndTimestamp[0].trim());

        return true;
    }



    @Override
    public LongWritable getCurrentKey() {
        return currentKey;
    }


    @Override
    public Relationship getCurrentValue() {
        return currentValue;
    }


    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }


    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
