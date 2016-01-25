package worker;
import messages.*;

/*
 * Default partitioner will be used if user submitting job does not specify specific partitioner to use.
 */
public class DefaultPartitioner extends PartitionerBase {

	public DefaultPartitioner(MasterMessage mm) {
		super(mm);
	}

	@Override
	protected int partition(KeyValuePair<String,String> kvp, int numPartitions) {
		int hashCode = kvp.getKey().hashCode() % numPartitions;
		return hashCode;
	}
}
