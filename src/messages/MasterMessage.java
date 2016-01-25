package messages;
import java.io.Serializable;
import java.util.ArrayList;


public class MasterMessage implements Serializable {

	private static final long serialVersionUID = 1L;
	//A user assigned name
	public String jobName;
	// operation is either map, reduce, partition, or exit. In the case of task completion message, will be set to completed.
	public String operation;
	// taskName will specify what mapper, partitioner, and reducer objects to run
	public String taskName;	
	//unique combination of identifiers
	public int jobID;
	public int taskID;
	//the input and output files
	public ArrayList<String> inputFileArray;
	public ArrayList<String> outputFileArray;
	//start and end records, specified only if the task is a mapper
	public int recordStart;
	public int recordEnd;
	
	public MasterMessage(String jobName, String operation, String taskName, int jobID,
			int taskID, ArrayList<String> inputFileArray,
			ArrayList<String> outputFileArray, int recordStart, int recordEnd) {
		this.jobName = jobName;
		this.operation = operation;
		this.taskName = taskName;
		this.jobID = jobID;
		this.taskID = taskID;
		this.inputFileArray = inputFileArray;
		this.outputFileArray = outputFileArray;
		this.recordStart = recordStart;
		this.recordEnd = recordEnd;
	}
	
	//Constructor to create completed message for completed tasks
	public MasterMessage(MasterMessage old) {
		this.jobName = old.jobName;
		this.operation = "completed";
		this.taskName = old.taskName;
		this.jobID = old.jobID;
		this.taskID = old.taskID;
		this.inputFileArray = old.inputFileArray;
		this.outputFileArray = old.outputFileArray;
		this.recordStart = old.recordStart;
		this.recordEnd = old.recordEnd;
	}
}