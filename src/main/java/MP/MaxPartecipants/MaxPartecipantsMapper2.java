package MP.MaxPartecipants;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class MaxPartecipantsMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    boolean check = true;
    String regex = "[0-9]+";
    Pattern p = Pattern.compile(regex);

    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        String[] row = value.toString().split(";");

        if (check) {
            check = false;
            return;
        }
        /*if (row[17].equals("Sending Organization") && row[21].equals("Receiving Organization")) {
            outputCollector.collect(new Text(row[17]), new Text(row[21]));
            return;
        }*/

        Matcher m = p.matcher(row[23]);

        if (!m.matches()) return;

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(row[21] + ";" + row[23]);

        outputCollector.collect(new Text(row[17]), new Text(stringBuilder.toString()));
    }
}
