package pymk_stage3;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class pymk3_mapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
      String[] str = value.toString().split("\t");
      String[] keystr = str[0].split(":"); 
      String[] pairstr = keystr[0].split(",");
   
      output.collect(new Text(pairstr[0] + ":" + keystr[1]), new Text(pairstr[1] + ":" + str[1]));
  }
}
