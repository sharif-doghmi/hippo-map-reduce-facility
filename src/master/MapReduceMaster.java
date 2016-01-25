package master;
import common.ConfigReader;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import messages.MasterMessage;

//the main Master Class
public class MapReduceMaster {


	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		ConcurrentHashMap<String,NodeInfo> nodeData = new ConcurrentHashMap<String, NodeInfo>();
		ConcurrentLinkedQueue<Task> taskQ = new ConcurrentLinkedQueue<Task>();
		ConcurrentHashMap<Integer, Job> runningJobs = new ConcurrentHashMap<Integer, Job>();
		//read the config file
		ConfigReader cr = new ConfigReader("config.txt");
		int hbport = cr.hbPort;
		int hbinterval = cr.hbInterval;
		int listenPort = cr.masterListenPort;
		int sendPort = cr.slaveListenPort;
		Thread nt, sch, sender;
		boolean stopping = false;
		Scanner scan = new Scanner(System.in);
		Socket sock;
		NodeInfo ni;
		
		//consider nodes dead until we hear a heartbeat
		for(int i=0; i<cr.slaveHosts.length; i++) {
			ni = new NodeInfo(cr.slaveCores[i]);
			ni.status = "dead";
			nodeData.put(cr.slaveHosts[i], ni);
		}
		
		//start the threads for the scheduler, node tracker, and task sender
		nt = new Thread(new NodeTracker(nodeData, hbport, hbinterval, taskQ));
		sch = new Thread(new Scheduler(nodeData, taskQ, runningJobs, listenPort));
		sender = new Thread(new TaskSender(sendPort, nodeData, taskQ));
		
		nt.start();
		sch.start();
		sender.start();
		
		
		String s;
		String[] split;
		MasterMessage mm;
		ObjectOutputStream oo;
		
		//Listen for commands from the user
		while(!stopping) {
			System.out.println("commands:");
			System.out.println("kill <hostname>");
			System.out.println("list");
			System.out.println("exit");
			s = scan.nextLine();
			split = s.split("\\s");
			if(s.equals("exit")) {
				//exit the program
				stopping = true;
				nt.stop();
				sch.stop();
				sender.stop();
				for(String host : nodeData.keySet()) {
					try {
						sock = new Socket(host, sendPort);
						mm = new MasterMessage(null, "exit", null, 0, 0, null, null, 0, 0);
						oo = new ObjectOutputStream(sock.getOutputStream());
						oo.writeObject(mm);
						oo.close();
						sock.close();
					} catch (Exception e) {
					}
				}
			} else if(s.equals("list")) {
				//list all running tasks on each node
				for(String host : nodeData.keySet()) {
					ni = nodeData.get(host);
					ni.lock.lock();
					try {
						System.out.println("Tasks running on " + host);
						for(MasterMessage rt : ni.taskList) {
							System.out.println(rt.jobName +"_"+ rt.operation+"_ID"+rt.taskID);
						}
					} finally {
						ni.lock.unlock();
					}
				}
			} else if(split[0].equals("kill") && nodeData.get(split[1])!=null) {
				try {
					//kill a specific worker node
					sock = new Socket(split[1], sendPort);
					mm = new MasterMessage(null, "exit", null, 0, 0, null, null, 0, 0);
					oo = new ObjectOutputStream(sock.getOutputStream());
					oo.writeObject(mm);
					oo.close();
					sock.close();
				} catch (Exception e) {
				}
			}
		}
		scan.close();
	}

}
