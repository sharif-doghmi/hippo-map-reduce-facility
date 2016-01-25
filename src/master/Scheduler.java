package master;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

//the scheduler, even though most scheduling happens in other classes
public class Scheduler implements Runnable {
	
	ConcurrentCounter jobIDCounter;
	int commPort;
	ConcurrentHashMap<String,NodeInfo> nodeData;
	ConcurrentLinkedQueue<Task> taskQ;
	ConcurrentHashMap<Integer, Job> runningJobs;

	
	public Scheduler(ConcurrentHashMap<String,NodeInfo> nd,
			ConcurrentLinkedQueue<Task> tq,
			ConcurrentHashMap<Integer,Job> rj,
			int port) {
		commPort = port;
		taskQ = tq;
		runningJobs = rj;
		jobIDCounter = new ConcurrentCounter();
		nodeData = nd;
	}
	
	//this is basically the listener
	//we listen for incoming jobs or task completion messages
	public void run() {
		ServerSocket ss;
		Socket s;
		Thread t;
		try {
			ss = new ServerSocket(commPort);
			while(true) {
				s = ss.accept();
				System.out.println("The Scheduler has received a message!");
				t = new Thread(new SlaveMsgHandler(s, jobIDCounter, nodeData, taskQ, runningJobs));
				t.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
