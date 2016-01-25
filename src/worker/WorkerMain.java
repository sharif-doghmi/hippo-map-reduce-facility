package worker;
import java.io.IOException;
import java.net.ServerSocket;

/*
 * Main class of worker process
 */
public class WorkerMain {

	// flag to tell main thread to exit. Set by other threads.
	public static volatile boolean exiting;
	
	public static void main(String[] args) throws InterruptedException, IOException {
		
		if(args==null || args.length!=1){
			System.out.println("Error: Improper Arguments");
			System.out.println("Usage: java workerMain <configFileName>");
			return;
		}

		// Initialize WorkerConfig object from config file. Done only once.
		new WorkerConfig(args[0]);
		
		// Create server socket
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(WorkerConfig.getListenPort());
			serverSocket.setSoTimeout(5000);
		} catch (IOException e) {
			System.out.println("Server socket initialization error");
			return;
		}
		
		// Create new worker listener server thread for handling messages from master and hand it server socket 
		WorkerListener workerListener = new WorkerListener(serverSocket);
		Thread wl = new Thread(workerListener);
		wl.start();
		System.out.println("Server initialized. Listening for incoming requests on port " + serverSocket.getLocalPort());
		
		// Create new worker heart beat thread and start it
		WorkerHeartBeat workerHeartBeat = new WorkerHeartBeat();
		Thread whb = new Thread(workerHeartBeat);
		whb.start();
		System.out.println("Heart beat thread initialized");
		
		// Create worker input monitor thread to accept input from local user
		Thread wim = new Thread(new WorkerInputMonitor());
		wim.start();
		
		// Test for exiting flag being set by other threads
		while (!exiting) {
			Thread.sleep(200);
		}
		
		/*
		 *  If main thread signaled to exit by other threads, then close threads and resources
		 */
		System.out.println("Worker terminating...");
		workerListener.exit();
		wl.join();
		serverSocket.close();
		System.out.println("Terminated server...");
		wim.interrupt();
		wim.join();
		System.out.println("Terminated user input monitor...");
		workerHeartBeat.exit();
		whb.join();
		System.out.println("Terminated heart beat thread...");
		System.out.println("Completed termination");
		}
}