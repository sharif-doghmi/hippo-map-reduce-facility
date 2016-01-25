package messages;
import java.io.Serializable;

//A message sent to the master by workers in order to start a new job
public class JobInitiationMessage implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public String jobName;
	public String inputFileName;
	public String mapperName;	// will specify what mapper object to run
	public String reducerName; // will specify what reducer object to run
	public String partitionerName; // optional
	
	public JobInitiationMessage(String jobName, String inputFileName, String mapperName, String reducerName, String partitionerName) {
		this.jobName = jobName;
		this.inputFileName = inputFileName;
		this.mapperName = mapperName;
		this.reducerName = reducerName;
		this.partitionerName = partitionerName;
	}
}