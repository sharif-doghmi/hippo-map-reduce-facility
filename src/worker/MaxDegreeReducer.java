package worker;

import java.util.ArrayList;

import messages.MasterMessage;

public class MaxDegreeReducer extends ReducerBase {

	public MaxDegreeReducer(MasterMessage mm) {
		super(mm);
	}

	@Override
	protected KeyValuePair<String, String> reduce(KeyValueListPair<String, String> kvlp) {
		ArrayList<String> valueList = kvlp.getValueList();
		Integer max = 0;
		for (String value : valueList) {
			if(Integer.valueOf(value) > max)
				max = Integer.valueOf(value);
		}
		KeyValuePair<String,String> kvp = new KeyValuePair<String,String>(kvlp.getKey(), max.toString());
		return kvp;
	}
}
