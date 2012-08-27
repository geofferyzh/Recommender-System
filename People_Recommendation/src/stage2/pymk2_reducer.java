

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class pymk2_reducer extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    String commonfr = "";
    int count = 0;
    // while loop to append a list 2nd degree connected friends
    while (values.hasNext()) {
      Text value = values.next();
      count = count +1;
      // deal with first reco in the list
      if (commonfr == "") {
	  commonfr = value.toString();
      } 
        else {
      	  if (commonfr.lastIndexOf(value.toString()) <0 ) {
      	    commonfr = commonfr + "," + value.toString();
      	  }
        }
    }
    // do not emit if pair of users are 1st degree connected
    if (commonfr.indexOf("1stD") < 0) {
      output.collect(new Text(key + ":" + Integer.toString(count)), new Text(Integer.toString(count) + ":" + commonfr));
    }
  }
}

