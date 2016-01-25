package worker;
import messages.*;
/*
 * Custom mapper created by application programmer that maps a 
 * key-value pair to a pair of the same key and a value of "1".
 * This is in preparation for the values to be summed by SumValuesReducer
 */
public class SumValuesMapper extends MapperBase {

	public SumValuesMapper(MasterMessage mm) {
		super(mm);
	}
	
	/*
	 * Custom implementation of abstract map method in base mapper class
	 */
	@Override
	protected KeyValuePair<String,String> map(KeyValuePair<String,String> kvp) {
		KeyValuePair<String,String> kvpEmit = new KeyValuePair<String,String>(kvp.getKey(), "1");
		return kvpEmit;
	}
}
