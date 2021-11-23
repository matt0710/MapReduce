package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

public class AvgAgeReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

        int size = 0;
        LinkedList<Integer> list = new LinkedList<>();
        int outcome = 0;


        while (iterator.hasNext()) {
            list.add(iterator.next().get());
            size++;
        }

        for (int i = 0; i < list.size(); i++) {
            outcome += list.get(i);
        }

        outputCollector.collect(text, new IntWritable(outcome/size));

    }
}
