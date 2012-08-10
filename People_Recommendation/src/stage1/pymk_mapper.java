
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
 
public class pymk_mapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
      String[] s = value.toString().split("\t");
        output.collect(new Text(s[0]), new Text(s[1]));
  }
}
