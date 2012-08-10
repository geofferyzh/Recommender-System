package pymk_stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;



// Partitions key based on "natural" key 
public class myPartitioner<K2, V2> implements Partitioner<Text,Text> {

        @Override
	public void configure(JobConf arg0) {
 
	}

	public int getPartition(Text key, Text value, int numPartitions) {
                String[] keyItems = key.toString().split(":"); 
                String[] keyUser  = keyItems.toString().split(",");
                String keyBase = keyUser[0];   
		int hash = keyBase.hashCode();
		int mypartition = hash % numPartitions;
		return mypartition;
	}

}