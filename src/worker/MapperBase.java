package worker;
import messages.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;

/*
 * Abstract standard base mapper class that is extended by application programmer to create custom mappers.
 * Mapper receives one input file and outputs one file.
 */
public abstract class MapperBase implements Runnable {
	
	private MasterMessage mm;
	
	public MapperBase(MasterMessage mm) {
		this.mm = mm;
	}
	
	/*
	 * Abstract map method that is to be defined in extended custom mapper class
	 */
	protected abstract KeyValuePair<String,String> map(KeyValuePair<String,String> kvp);
	
	/*
	 * Write data to output file
	 */
	private void emit(KeyValuePair<String,String> kvp, PrintWriter writer) {
		writer.println(kvp.getKey() + " " + kvp.getValue());
	}
	@Override
	public void run () {
		System.out.println("Running mapper " + mm.taskName + " with ID " + mm.taskID);
		int i = 0;
		try (FileReader fr = new FileReader(mm.inputFileArray.get(0));
				BufferedReader br = new BufferedReader(fr);
				PrintWriter pw = new PrintWriter(mm.outputFileArray.get(0), "UTF-8")) 
			{
			
			// Skip to line number required as indicated in master message
			for (i = 1; i < mm.recordStart; i++) {
				br.readLine();
			}
			
			// Map and output each key-value pair one by one. Each pair is on one line in input and output files.
			for (i = mm.recordStart; i <= mm.recordEnd; i++) {
				String line = br.readLine();
				String keyValueArray[] = line.split("\\s");
				KeyValuePair<String,String> kvp = new KeyValuePair<String,String>(keyValueArray[0], keyValueArray[1]);
				emit(map(kvp), pw);
			}
			System.out.println("Mapper " + mm.taskName + " with ID " + mm.taskID + " finished execution");
			sendCompletionMessage();
		} catch (FileNotFoundException e) {
			System.out.println("Mapper " + mm.taskID + " cannot open file ");
		}
		catch (IOException e) {
			System.out.println("Mapper " + mm.taskID + " cannot read line " + i + " in input file " + mm.inputFileArray.get(0) );
		}
	}
	
	/*
	 * Tell master this mapping task is completed
	 */
	private void sendCompletionMessage() {
		MasterMessage cm = new MasterMessage(mm);
		try (Socket soc = new Socket(WorkerConfig.getMasterHostName(), WorkerConfig.getMasterPort());
				ObjectOutputStream oo = new ObjectOutputStream(soc.getOutputStream());) {
				oo.writeObject(cm);
				System.out.println("Mapper " + mm.taskID + " sent task completion message to master");
		} catch (IOException e) {
			System.out.println("Mapper " + mm.taskID + " failed to send task completion message to master");
		}
	}
}