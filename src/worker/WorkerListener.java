package worker;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/*
 * Server on worker that listens for incoming connections from master and launches threads to handle them
 */
public class WorkerListener implements Runnable {

	ServerSocket serverSocket;
	String masterHostName;
	private volatile boolean exiting;
	
	public WorkerListener(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
		masterHostName = WorkerConfig.getMasterHostName();
	}
	
	public void run() {
		InetAddress masterAddress = null;
		// Get IP address of master from master's host name
		try {
			masterAddress = InetAddress.getByName(masterHostName);
		} catch (UnknownHostException e1) {
			System.out.println("Illegal master host name in config file");
			WorkerMain.exiting = true;
			return;
		}
		
		/*
		 * listen for incoming connections while not signaled by main thread to exit
		 */
		while (!exiting) {
			try{
				//Accept connections on server socket
				Socket connectionSocket = serverSocket.accept();
				InetAddress connectionAddress = connectionSocket.getInetAddress();
				
				/* Check IP of remote connecting node and if it is the master, then create and launch thread
				 * to handle connection.
				 */
				if (connectionAddress.equals(masterAddress)) { 
			 		System.out.println("Received connection from master at address:" + connectionAddress.toString());
			 		HandleMasterWorkerMessage hmwm = new HandleMasterWorkerMessage(connectionSocket);
			 		Thread t = new Thread(hmwm);
			 		t.start();
			 	} else {
			 		System.out.println("Refused connection request from unknown host at address:" + connectionAddress.toString());
			 		connectionSocket.close();
			 	}
			} catch(IOException e) {
				//do nothing since the server socket probably just timed out without getting connection
			}
		}
	}
	
	//main invokes this method to tell this thread to terminate
	public void exit() {
		exiting = true;
	}
}