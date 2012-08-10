package pymk_stage2;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class pymk2_mapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
      String[] s = value.toString().split("\t");
      String[] vs = s[1].split("(?<!\\\\),"); 
      List<String> vlist = Arrays.asList(vs);   
//      for (String v : vlist) {
// 	  output.collect(new Text(s[0] + "," + v), new IntWritable(1));
//    }
      for (int i=0; i < vlist.size(); i++) {
      	output.collect(new Text(s[0] + "," + vlist.get(i)), new Text("1stD"));
	for (int j= i+1; j < vlist.size(); j++) {
	 output.collect(new Text(vlist.get(i) + "," + vlist.get(j)), new Text(s[0]));
	 output.collect(new Text(vlist.get(j) + "," + vlist.get(i)), new Text(s[0]));   
        }
      }
  }
}

