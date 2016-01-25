package master;
import messages.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//our representation of a job - something a user starts
public class Job {
	//unique id
	int jobID;
	//user given name
	String jobName;
	//specific mapper function
	String mapName;
	//specific reducer function
	String reduceName;
	//specific partition function (optional)
	String partitionName;
	//the initial input file
	String inputFileName;
	//maps to keep track of the three types of tasks associated with each job
	ConcurrentHashMap<Integer, MasterMessage> mapTasks;
	ConcurrentHashMap<Integer, MasterMessage> partitionTasks;
	ConcurrentHashMap<Integer, MasterMessage> reduceTasks;
	//how many map=>partition tasks have been completed
	int numMapPartDone;
	//how many reduces have completed
	int numReduceDone;
	//how many map=>partition tasks there were total
	int totalMapPart;
	//how many reduce tasks there were total
	int totalReduce;
	//a counter that lets us assign task IDs
	int taskCounter;
	//a lock for concurrency
	Lock lock;
	
	public Job(String jn, int jid, String map, String red, String part, String inFile) {
		jobName = jn;
		jobID = jid;
		mapName = map;
		reduceName = red;
		partitionName = part;
		inputFileName = inFile;
		taskCounter = 0;
		numMapPartDone = 0;
		numReduceDone = 0;
		totalReduce = -1;
		mapTasks = new ConcurrentHashMap<Integer,MasterMessage>();
		partitionTasks = new ConcurrentHashMap<Integer,MasterMessage>();
		reduceTasks = new ConcurrentHashMap<Integer,MasterMessage>();
		lock = new ReentrantLock();
	}
}
