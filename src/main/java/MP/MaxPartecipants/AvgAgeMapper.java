package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AvgAgeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    boolean check = true;
    String regex = "[0-9]+";
    Pattern p = Pattern.compile(regex);

    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

        String[] row = value.toString().split(";");

        if (check) {
            check = false;
            return;
        }

        Matcher m = p.matcher(row[14]);

        if (!m.matches()) return;

        outputCollector.collect(new Text(row[7]), new IntWritable(Integer.parseInt(row[14])));
    }
}
