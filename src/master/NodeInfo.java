package master;
import messages.*;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//info about each worker node
public class NodeInfo {
	//the number of cores
	int maxTasks;
	//alive or dead
	String status;
	//tasks that are currently running on the node
	ArrayList<MasterMessage> taskList;
	//the last know heartbeat from the node
	long lastHB;
	//lock for concurrency
	Lock lock;
	
	public NodeInfo(int mt) {
		lastHB = 0;
		taskList = new ArrayList<MasterMessage>();
		maxTasks = mt;
		lock = new ReentrantLock();
	}
}
