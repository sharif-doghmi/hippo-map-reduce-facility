package master;
import messages.*;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


public class SlaveMsgHandler implements Runnable {
	//for communication
	Socket s;
	//how many jobs have been started
	ConcurrentCounter jobIDCounter;
	//tracking information about each worker
	ConcurrentHashMap<String, NodeInfo> nodeData;
	//tasks to be sent out to workers
	ConcurrentLinkedQueue<Task> taskQ;
	//jobs that have been received
	ConcurrentHashMap<Integer, Job> runningJobs;
	
	public SlaveMsgHandler(Socket s, ConcurrentCounter jobIDCounter,
			ConcurrentHashMap<String, NodeInfo> nodeData,
			ConcurrentLinkedQueue<Task> taskQ,
			ConcurrentHashMap<Integer, Job> runningJobs) {
		this.s = s;
		this.jobIDCounter = jobIDCounter;
		this.taskQ = taskQ;
		this.nodeData = nodeData;
		this.runningJobs = runningJobs;
		
		}

	@Override
	public void run() {
		
		ObjectInputStream oi;
		Object message;
		Class<?> c;
		JobInitiationMessage jim;
		MasterMessage mm, rm;
		Job aJob;
		NodeInfo ni;
		InetSocketAddress remoteAddr = (InetSocketAddress) s.getRemoteSocketAddress();
		
		try {
			//read in the object and check what type of message it is
			oi = new ObjectInputStream(s.getInputStream());
			message = oi.readObject();
			c = message.getClass();
			
			if(c.equals(JobInitiationMessage.class)) {
				//this is a new job
				jim = (JobInitiationMessage)message;
				System.out.println("The new job " + jim.jobName + " has arrived");
				jobIDCounter.lock.lock();
				try {
					//create a Job object to represent the new job
					aJob = new Job(jim.jobName,jobIDCounter.counter, jim.mapperName, 
							jim.reducerName, jim.partitionerName, jim.inputFileName);
					runningJobs.put(jobIDCounter.counter, aJob);
					jobIDCounter.counter++;
				} finally {
					jobIDCounter.lock.unlock();
				}
				//need to add mappers to taskq
				addMaps(aJob);
				
			} else if (c.equals(MasterMessage.class)) {
				//our node finished a task
				mm = (MasterMessage)message;
				ni = nodeData.get(remoteAddr.getHostName().toLowerCase());
				ni.lock.lock();
				//update our node information and remove the task since it has finished
				try {
					for(int i=0;i< ni.taskList.size(); i++) {
						rm = ni.taskList.get(i);
						if(rm.jobID==mm.jobID && rm.taskID==mm.taskID)
							ni.taskList.remove(rm);
					}
				} finally {
					ni.lock.unlock();
				}
				//update the Job information and check its current status
				aJob = runningJobs.get(mm.jobID);
				aJob.lock.lock();
				try {
					if(aJob.mapTasks.get(mm.taskID)!=null) {
						//a map task has finished, need to launch partitioner
						System.out.println(mm.taskName+", task ID "+mm.taskID+", from "+aJob.jobName+" has finished");
						mapHandle(aJob, mm);
					} else if(aJob.partitionTasks.get(mm.taskID)!=null) {
						//a partition task has finished, if all maps+partitions done need to launch reducers
						System.out.println("Partition task ID "+mm.taskID+", from "+aJob.jobName+" has finished");
						partHandle(aJob, mm);
					} else if(aJob.reduceTasks.get(mm.taskID)!=null) {
						//a reduce task has finished, if all reducers done then Job is complete
						System.out.println(mm.taskName+", task ID "+mm.taskID+", from "+aJob.jobName+" has finished");
						reduceHandle(aJob, mm);
					} else {
						//this should never happen.
						System.out.println("unknown task completed for " + aJob.jobName + " with mapper " 
								+ aJob.mapName + " and reducer " + aJob.reduceName);
					}
				} finally {
					aJob.lock.unlock();
				}
			} else {
				//this should never happen
				System.out.println(c.toString());
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	//this method executes when a reduce task has been finished by a node
	private void reduceHandle(Job aJob, MasterMessage mm) {
		aJob.lock.lock();
		try {
			aJob.numReduceDone++;
			if(aJob.numReduceDone==aJob.totalReduce) {
				//launch global reducer
				globalReduce(aJob);
			} else if(aJob.numReduceDone>aJob.totalReduce) {
				//The job is finished
				jobsDone(aJob);
			}
		} finally {
			aJob.lock.unlock();
		}
	}
	//This creates the gloal reduce task, the last part of our mapreduce
	private void globalReduce(Job aJob) {
		MasterMessage mm;
		Task t;
		aJob.lock.lock();
		//build and queue the message
		try {
			mm = new MasterMessage(aJob.jobName, "reduce", aJob.reduceName, aJob.jobID,
					aJob.taskCounter, new ArrayList<String>(), new ArrayList<String>(), 0,0);
			mm.outputFileArray.add("Result_"+mm.jobName+".out");
			for(int reduceID : aJob.reduceTasks.keySet()) {
				mm.inputFileArray.add(aJob.reduceTasks.get(reduceID).outputFileArray.get(0));
			}
			aJob.taskCounter++;
			aJob.reduceTasks.put(mm.taskID, mm);
			t = new Task(null, mm);
			taskQ.add(t);
		} finally {
			aJob.lock.unlock();
		}

	}

	//This method prints job completion information
	private void jobsDone(Job aJob) {
		MasterMessage mm;
		
		System.out.println("Job Completed: " + aJob.jobName);
		System.out.println("\t Job ID: " + aJob.jobID);
		System.out.println("\t Mapper: " + aJob.mapName);
		System.out.println("\t Reducer: " + aJob.reduceName);
		if(aJob.partitionName != null)
			System.out.println("\t Partitioner: " + aJob.partitionName);
		System.out.println("\t Input File: " + aJob.inputFileName);
		System.out.print("\t Output File: ");
		for(Enumeration<MasterMessage> e = aJob.reduceTasks.elements(); e.hasMoreElements();) {
			mm = e.nextElement();
			for(String s : mm.outputFileArray) {
				if(!s.contains(".tmp") && s.contains(".out"))
					System.out.println(s);
			}
		}
		//remove the complete job from our list of running jobs.
		runningJobs.remove(aJob.jobID);
	}

	//this method executes when a partition task has completed
	private void partHandle(Job aJob, MasterMessage mm) {
		aJob.lock.lock();
		try {
			aJob.numMapPartDone++;
			if(aJob.numMapPartDone==aJob.totalMapPart) {
				//all mapping and partitioning is done, time to queue up reducers
				launchReducers(aJob);
			}
		} finally {
			aJob.lock.unlock();
		}
		
	}
	//when all mappers and partitions have been complete, this method 
	//launches the appropriate number of reducer tasks
	private void launchReducers(Job aJob) {
		System.out.println("All Maps and Partitions have finished for " + aJob.jobName);
		int numReducers = aJob.totalReduce;
		MasterMessage mm;
		Task t;
		
		//aggregate input files for each reducer
		ArrayList<ArrayList<String>> inputLists = new ArrayList<ArrayList<String>>();
		for(int i=0; i<numReducers; i++) {
			inputLists.add(new ArrayList<String>());
		}
		for(Enumeration<MasterMessage> e = aJob.partitionTasks.elements(); e.hasMoreElements();) {
			mm = e.nextElement();
			for(int i=0; i< mm.outputFileArray.size(); i++) {
				inputLists.get(i).add(mm.outputFileArray.get(i));
			}
		}

		//build and queue the message
		aJob.lock.lock();
		try {
			for(int i=0; i<numReducers; i++) {
				mm = new MasterMessage(aJob.jobName, "reduce", aJob.reduceName, aJob.jobID,
						aJob.taskCounter, inputLists.get(i), new ArrayList<String>(), 0,0);
				mm.outputFileArray.add("ReduceOut_Job"+mm.jobID+"_File"+i+".tmp");
				aJob.reduceTasks.put(mm.taskID, mm);
				aJob.taskCounter++;
				t = new Task(null, mm);
				taskQ.offer(t);
			}
		} finally {
			aJob.lock.unlock();
		}
		
	}

	//This method executes when a map task has finished
	private void mapHandle(Job aJob, MasterMessage mm) {
		int numReducers = 0;
		NodeInfo ni;
		Task t;
		InetSocketAddress remoteAddr= (InetSocketAddress) s.getRemoteSocketAddress();
		String outFileStart;
		
		MasterMessage partition;

		//start the partition on the output of the map
		aJob.lock.lock();
		try {
			partition = new MasterMessage(aJob.jobName,"partition", aJob.partitionName,  
					aJob.jobID, aJob.taskCounter, mm.outputFileArray, new ArrayList<String>(), 0,0);
			aJob.taskCounter++;
			//if we don't yet know how many reducers we need to calculate that
			if(aJob.totalReduce==-1) {
				for(Enumeration<NodeInfo> e = nodeData.elements(); e.hasMoreElements();) {
					ni = e.nextElement();
					ni.lock.lock();
					try {
						if(ni.status.equals("alive") && ni.maxTasks>ni.taskList.size()) {
							numReducers++;
						}
					} finally {
						ni.lock.unlock();
					}
				}
				if(numReducers<1) {
					numReducers++;
				}
				aJob.totalReduce = numReducers;
			} else {
				numReducers = aJob.totalReduce;
			}

			//name the output files
			outFileStart = "partOut_Job"+partition.jobID+"_Task"+partition.taskID+"_File";

			for(int i=0; i<numReducers; i++) {
				partition.outputFileArray.add(outFileStart+i+".tmp");
			}

			aJob.partitionTasks.put(partition.taskID, partition);
			//queue up the partition task
			t = new Task(remoteAddr.getHostName().toLowerCase(), partition);
			taskQ.offer(t);		
		} finally {
			aJob.lock.unlock();
		}
	}

	//when a new job is received, this method chooses how many maps to launch
	private void addMaps(Job newJob) throws IOException {
		System.out.println("Calculating mappers for "+newJob.jobName);
		int totalRecords, maxMappers, availableMaps, mapsToAdd, startRec, endRec, recPerMap;
		Task t;
		MasterMessage mm;
		NodeInfo ni;
		LineNumberReader lineRead = new LineNumberReader(new FileReader(newJob.inputFileName));
		//examine the input file for size
		while(lineRead.readLine()!=null);
		totalRecords = lineRead.getLineNumber();
		lineRead.close();
		//no fewer than 500 records per map
		maxMappers = totalRecords/500;
		
		//calculate how many cores are available across our nodes
		availableMaps = 0;
		for(Enumeration<NodeInfo> e = nodeData.elements(); e.hasMoreElements();) {
			ni = e.nextElement();
			ni.lock.lock();
			try {
				if(ni.status.equals("alive"))
					availableMaps += (ni.maxTasks-ni.taskList.size());
			} finally {
				ni.lock.unlock();
			}
		}
		if(availableMaps < nodeData.size()) availableMaps = nodeData.size();
		if(maxMappers < 1) maxMappers = 1;
		
		//make sure we don't have too many maps
		mapsToAdd = (availableMaps<maxMappers ? availableMaps : maxMappers);
		recPerMap = totalRecords/mapsToAdd;
		startRec = 1;
		endRec = startRec+recPerMap+1;
		
		//start creating map tasks
		newJob.lock.lock();
		try {
			for(int i=0; i<mapsToAdd; i++) {
				if(startRec>totalRecords) break;
				if(endRec>totalRecords || i==mapsToAdd-1) endRec = totalRecords;
				mm = new MasterMessage(newJob.jobName,"map", newJob.mapName, newJob.jobID, 
						newJob.taskCounter, new ArrayList<String>(), new ArrayList<String>(), startRec, endRec);
				mm.inputFileArray.add(newJob.inputFileName);
				mm.outputFileArray.add("mapOut_Job"+mm.jobID+"_Task"+mm.taskID+".tmp");
				newJob.mapTasks.put(mm.taskID, mm);
				newJob.taskCounter++;
				t = new Task(null, mm);
				taskQ.offer(t);
				startRec = endRec + 1;
				endRec = startRec + recPerMap+1;
			}
			newJob.totalMapPart = newJob.taskCounter;
		} finally {
			newJob.lock.unlock();
		}
	}

}
