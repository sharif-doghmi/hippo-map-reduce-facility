package worker;
import messages.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;

/*
 * Worker input monitor accepts input from local user on worker node and processes it
 */
public class WorkerInputMonitor implements Runnable {

	String masterHostName;
	int masterPort;
	
	public WorkerInputMonitor() {
		this.masterHostName = WorkerConfig.getMasterHostName();
		this.masterPort = WorkerConfig.getMasterPort();
	}
	
	@Override
	public void run() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in)); // For reading user input
	    String input = null;
	    String[] arr = null;
	    do {
	      System.out.println("Start a new job or type \"exit\" to terminate worker:");
	      System.out.println("To start a new job, enter: \"start <jobName> <inputFileName> <mapperName> <reducerName> <partitionerName>\"");
	      System.out.println("jobName can be arbitrary. PartitionerName is optional.");
	      try {
	        while (!br.ready()) { // Loop until user input is available, sleeping between loops
	          Thread.sleep(200);
	        }
		    input = br.readLine(); // read user input
		    arr = input.split("\\s");
		    String partitioner = null;
		    switch (arr[0]) {	// Determine next course of action based on first word in user input
		    	case "exit":	// Local user wishes to terminate worker process
		    		WorkerMain.exiting = true;
		    		return;
		    	case "start":	// User wishes to start a MapReduce job
		    		if ((arr.length != 5) && (arr.length != 6)) {
		    			System.out.println("Wrong number of arguments for job start command");
		    		} else {
		    			if (arr.length == 6) {
		    				partitioner = arr[5];	// User specified a custom partitioner
		    			} else if (arr.length == 5) {
		    				partitioner = "DefaultPartitioner";	// Use default partitioner
		    			}
		    			
		    			/* 
		    			 * Create and send job initiation message to master
		    			 */
		    			JobInitiationMessage jim = 
		    				new JobInitiationMessage(arr[1], arr[2], arr[3], arr[4], partitioner);
		    			try (Socket soc = new Socket(masterHostName, masterPort);
		    				ObjectOutputStream oo = new ObjectOutputStream(soc.getOutputStream());) {
		    				oo.writeObject(jim);
		    				System.out.println("Sent job initiation message to master");
		    			} catch (IOException e) {
		    				System.out.println("WorkerInputMonitor network communication error with master");
		    			}
		    		}
		    		break;
		    	default:
		    		System.out.println("Invalid input");	// User entered invalid input
		    		break;
		    }
		  } catch (InterruptedException e) {	// Main thread sent interrupt to this thread. Terminate thread. 
		        return;
		  } catch (IOException e) {
				System.out.println("WorkerInputMonitor experienced IOException");
		  }
		} while (!arr[0].equals("exit"));	
			WorkerMain.exiting = true;		    
	}
}