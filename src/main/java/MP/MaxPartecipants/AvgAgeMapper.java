package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class AvgAgeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    boolean check = true;

    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {


        String[] row = value.toString().split(";");

        if (check) {
            check = false;
            return;
        }

        if (row[7].equals("Participant Nationality") && row[14].equals("Participant Age")) {
            outputCollector.collect(new Text(row[7]), new Text(row[14]));
            return;
        }

        outputCollector.collect(new Text(row[7]), new Text(row[14]));


    }
}
