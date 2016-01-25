package master;
import messages.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


public class NodeTracker implements Runnable {

	ConcurrentHashMap<String,NodeInfo> nodeData;
	int hbport;
	int hbinterval;
	ConcurrentLinkedQueue<Task> taskQ;
	
	public NodeTracker(ConcurrentHashMap<String,NodeInfo> nd, int hbport, int hbinterval,
				ConcurrentLinkedQueue<Task> taskQ) {
		nodeData = nd;
		this.hbport = hbport;
		this.hbinterval = hbinterval;
		this.taskQ = taskQ;
		
	}
	
	//continuously monitor the last known heartbeat for each node
	public void run() {
		NodeInfo ni;
		Thread t = new Thread(new HBListener(hbport, hbinterval, nodeData));
		t.start();

		while(true){
			for(String host : nodeData.keySet()) {
				ni = nodeData.get(host);
				ni.lock.lock();
				try {
					if(ni.status.equals("alive") && System.currentTimeMillis() - ni.lastHB > (long)3*hbinterval) {
						//node is dead
						ni.status = "dead";
						System.out.println("The Node Tracker has not received a heartbeat from node " 
								+ host + " in some time and now considers it dead.");
						for(MasterMessage mm : ni.taskList) {
							taskQ.offer(new Task(null, mm));
						}
					} else if(ni.status.equals("dead") && System.currentTimeMillis() - ni.lastHB <= (long)3*hbinterval){
						//node is alive again
						System.out.println("The Node Tracker has received a heartbeat from node " 
								+ host + " which was previously considered dead.");
						ni.status = "alive";
					}
				} finally {
					ni.lock.unlock();
				}
			}

		}

	}
	
}
