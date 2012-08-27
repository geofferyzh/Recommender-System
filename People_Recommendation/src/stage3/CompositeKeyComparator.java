

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Compares the composite key
public class CompositeKeyComparator extends WritableComparator {

	/*s Constructor. */
	protected CompositeKeyComparator() {
		super(Text.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text k1 = (Text)w1;
		Text k2 = (Text)w2;

                String[] k1Items = k1.toString().split(":");  
                String[] k2Items = k2.toString().split(":");  

	  	String k1Base = k1Items[0];  
		String k2Base = k2Items[0];  
		
		int comp = k1Base.compareTo(k2Base);

		if(0 == comp) {
			comp = -1 * k1Items[1].compareTo(k2Items[1]);
		}
		return comp;
	}
}

