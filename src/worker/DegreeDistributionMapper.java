package worker;

import messages.MasterMessage;

/*
 * This custom mapper created by the application programmer receives a (key,value) pair and outputs
 * the corresponding (value, "1") pair. It can be used in the second stage of a 2 step graph mining job to
 * calculate the degree distribution of vertices in a graph.
 */
public class DegreeDistributionMapper extends MapperBase {

	public DegreeDistributionMapper(MasterMessage mm) {
		super(mm);
	}

	@Override
	protected KeyValuePair<String, String> map(KeyValuePair<String, String> kvp) {
		KeyValuePair<String,String> kvpEmit = new KeyValuePair<String,String>(kvp.getValue(), "1");
		return kvpEmit;
	}

}
