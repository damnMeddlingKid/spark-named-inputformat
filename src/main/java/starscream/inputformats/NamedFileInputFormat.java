package starscream.inputformats;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class NamedFileInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new NamedFileRecordReader(jobConf,(FileSplit)inputSplit);
    }
}
