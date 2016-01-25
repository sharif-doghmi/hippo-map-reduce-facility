package worker;
import messages.*;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

/*
 * Worker heart beat thread submits heart beats at regular intervals to master
 */
public class WorkerHeartBeat implements Runnable {
	
	String masterHostName;
	int masterHBPort;
	int heartBeatPeriod;
	private volatile boolean exiting;
	
	public WorkerHeartBeat() {
		this.masterHostName = WorkerConfig.getMasterHostName();
		this.masterHBPort = WorkerConfig.getMasterHBPort();
		this.heartBeatPeriod = WorkerConfig.getHeartBeatPeriod();
	}
	public void run() {
		HeartBeatMessage hbm = new HeartBeatMessage(); // Create heart beat message
		while (!exiting) {	// Loop until signaled by main thread to exit
			try {
				Thread.sleep(heartBeatPeriod);	// sleep for heart period interval
			} catch (InterruptedException e) {
				System.out.println("WorkerHeartBeat experienced Interrupted Exception.");
			}
			//Send heart beat message to master
			try (Socket soc = new Socket(masterHostName, masterHBPort);
				ObjectOutputStream oo = new ObjectOutputStream(soc.getOutputStream());) {
				oo.writeObject(hbm);
			} catch(IOException e) {	
				 System.out.println("WorkerHeartBeat has experienced network communication error with master.");
			}
		}
	}
	//main invokes this method to tell this thread to terminate
	public void exit() {
		exiting = true;
	}
}