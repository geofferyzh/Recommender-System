

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class pymk3_reducer extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    String[] keystr = key.toString().split(":");
    int topcnt = 0;
    while (values.hasNext() && topcnt <20) {
      Text value = values.next();  
      String[] valuestr = value.toString().split(":");
      output.collect(new Text(keystr[0] + "," + valuestr[0]), new Text(valuestr[1] + ":" + valuestr[2])); 
      topcnt += 1;
    }
  }
}

