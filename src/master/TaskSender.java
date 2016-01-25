package master;
import messages.MasterMessage;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

//This class reads tasks from a concurrent queue and sends them to available nodes
public class TaskSender implements Runnable {
	//info about each node
	ConcurrentHashMap<String, NodeInfo> nodeData;
	//the Task Queue
	ConcurrentLinkedQueue<Task> taskQ;
	Queue<String> nodeQ;
	//Slave listening port
	int port;
	
	public TaskSender(int port, ConcurrentHashMap<String, NodeInfo> nodeData,
			ConcurrentLinkedQueue<Task> taskQ) {

		this.nodeData = nodeData;
		this.taskQ = taskQ;
		this.port = port;
		nodeQ = new ArrayBlockingQueue<String>(nodeData.size());
		for(String nodeName : nodeData.keySet()) {
			nodeQ.offer(nodeName);
		}
	}

	public void run() {
		Task t;
		int nodes = nodeQ.size();
		NodeInfo ni;
		String nodeName;
		boolean unlaunched;
		//check the task queue for new tasks then assign each one to an available node
		while(true) {
			if(taskQ.peek()!=null) {
				unlaunched = true;
				t = taskQ.peek();
				//if the task is marked for a specific worker then send it there, 
				//otherwise just choose an available node
				if(t.targetHost!=null) {
					ni = nodeData.get(t.targetHost);
					if(ni.status.equals("alive") && ni.maxTasks>ni.taskList.size()) {
						if(launchTask(t.targetHost, t.mm)>0) {
							taskQ.poll();
							unlaunched = false;
						}
					} 
				} else {
					for(int i=0; i<nodes; i++) {
						nodeName = nodeQ.poll();

						nodeQ.offer(nodeName);
						ni = nodeData.get(nodeName);						
	
						if(unlaunched && ni.status.equals("alive") && ni.maxTasks>ni.taskList.size()) {
							if(launchTask(nodeName, t.mm)>0) {
								taskQ.poll();
								unlaunched = false;
								break;
							}
						} 
					}
				}
				if(unlaunched) taskQ.offer(taskQ.poll());
			}
		}
	}

	//this method handles the actual sending of the message to the worker
	private int launchTask(String targetHost, MasterMessage mm) {
		System.out.println("Launching " + mm.taskName + " task ID " + 
				mm.taskID +" from " + mm.jobName + " to node " + targetHost);
		try{
			Socket s = new Socket(targetHost, port);
			ObjectOutputStream oo = new ObjectOutputStream(s.getOutputStream());
			oo.writeObject(mm);
			oo.close();
			s.close();
			nodeData.get(targetHost).taskList.add(mm);
			return 1;
		} catch (Exception e) {
			return -1;
		} 
	}

}
