package worker;
import messages.*;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

/*
 * Thread launched by worker listener thread for each connection accepted from master.
 * Master can either launch a map/partition/reduce task on the worker, or instruct the worker
 * to terminate. Each task requested will be launched in a separate thread.
 */
public class HandleMasterWorkerMessage implements Runnable {
	Socket socket;
	
	public HandleMasterWorkerMessage (Socket socket) {
		this.socket = socket;
	}
	@Override
	public void run() {
		ObjectInputStream oi = null;
		
		/*
		 * Read master message and process it, then close socket.
		 */
		try  {
			oi = new ObjectInputStream(socket.getInputStream());
			MasterMessage mm = (MasterMessage) oi.readObject();
			chooseTask(mm);
			oi.close();
			socket.close();
		} catch (Exception e) {
			System.out.println("HandleMasterWorkerMessage caught exception.");
		}
	}
	public void chooseTask(MasterMessage mm) throws InterruptedException {
		Thread t;

		/*
		 * Switch case determines what course of action to take based on operation field of master message
		 */
		try {
		switch (mm.operation.toLowerCase()) {
		case "map":
		case "reduce":
		case "partition":
			/*
			 * Use reflection to determine the class of the object (which mapper/reducer/partitioner) 
			 * to be instantiated based on the task name field of master message
			 */
			Class<?> taskClass;
			taskClass = Class.forName("worker."+mm.taskName);
			Class<?> argArray = MasterMessage.class;
			
			/*
			 * Construct requested task object and launch it in a separate thread
			 */
			Constructor<?> con = taskClass.getConstructor(argArray);
			t = new Thread((Runnable) con.newInstance(mm));
			t.start();
			break;
		case "exit":	// Master requested worker to terminate.
			WorkerMain.exiting = true;
			break;
		default:
			System.out.println("Invalid message from master.");
			break;
		}
		} catch (ClassNotFoundException e) {
			System.out.println("HandleMasterWorkerMessage encountered ClassNotFoundException");
		} catch (NoSuchMethodException e) {
			System.out.println("HandleMasterWorkerMessage encountered NoSuchMethodException");
		} catch (InstantiationException|IllegalArgumentException|IllegalAccessException|InvocationTargetException e) {
			System.out.println("HandleMasterWorkerMessage encountered exception from constructor");
		}
	}
}