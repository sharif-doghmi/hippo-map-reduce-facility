package worker;
import messages.*;
import java.util.ArrayList;

/*
 * Custom reducer created by application programmer that receives a pair 
 * of key and list of values and outputs a pair of the same key and the sum 
 * of values in the list
 */
public class SumValuesReducer extends ReducerBase {

	public SumValuesReducer(MasterMessage mm) {
		super(mm);
	}

	/*
	 * Custom implementation of abstract reduce method in abstract base reducer class
	 */
	@Override
	protected KeyValuePair<String,String> reduce(KeyValueListPair<String, String> kvlp) {
		ArrayList<String> valueList = kvlp.getValueList();
		Integer sum = 0;
		for (String value : valueList) {
			sum += Integer.valueOf(value);
		}
		KeyValuePair<String,String> kvp = new KeyValuePair<String,String>(kvlp.getKey(), sum.toString());
		return kvp;
	}
}
