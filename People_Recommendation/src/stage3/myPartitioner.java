

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;
import java.lang.*;


// Partitions key based on "natural" key 
public class myPartitioner<K2, V2> implements Partitioner<Text,Text> {

        @Override
	public void configure(JobConf job) {
	}

	public int getPartition(Text key, Text value, int numPartitions) {
                String[] keyItems = key.toString().split(":"); 
                String keyBase = keyItems[0];   
		int hash = keyBase.hashCode() & Integer.MAX_VALUE;
		int mypartition = hash % numPartitions;
		return mypartition;
	}

}
