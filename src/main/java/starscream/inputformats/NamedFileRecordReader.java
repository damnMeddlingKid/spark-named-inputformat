package starscream.inputformats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class NamedFileRecordReader implements RecordReader<Text, Text> {

    private LineRecordReader lineReader;
    private LongWritable lineKey;
    private Text lineValue;
    private String fileName;

    public NamedFileRecordReader(JobConf jobConf, FileSplit inputSplit) throws IOException {
        lineReader = new LineRecordReader(jobConf, inputSplit);
        lineKey = lineReader.createKey();
        lineValue = lineReader.createValue();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    public boolean next(Text key, Text value) throws IOException {
        if(!lineReader.next(lineKey, lineValue)) {
            return false;
        }
        value.set(lineValue);
        return true;
    }

    @Override
    public Text createKey() {
        return new Text(fileName);
    }

    @Override
    public Text createValue() {
        return new Text("");
    }

    @Override
    public long getPos() throws IOException {
        return lineReader.getPos();
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return lineReader.getProgress();
    }
}
