package worker;

import messages.MasterMessage;

public class MaxDegreeMapper extends MapperBase {

	public MaxDegreeMapper(MasterMessage mm) {
		super(mm);
	}

	@Override
	protected KeyValuePair<String, String> map(KeyValuePair<String, String> kvp) {
		KeyValuePair<String,String> kvpEmit = new KeyValuePair<String,String>("0", kvp.getValue());
		return kvpEmit;
	}
}
