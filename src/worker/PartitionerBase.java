package worker;
import messages.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;

/*
 * Abstract base partitioner that can be extended by the application programmer to create a custom partitioner.
 * The default partitioner also extends this class and will be used if the user does not specify
 * a specific partitioner to use.
 * Partitioner receives one input file (output of mapper) and creates multiple output files according to number of
 * partitions determined by master.
 */
public abstract class PartitionerBase implements Runnable {

	private MasterMessage mm;
	
	public PartitionerBase(MasterMessage mm) {
		this.mm = mm;
	}
	
	protected abstract int partition(KeyValuePair<String,String> kvp, int numPartitions);
	
	private void emit(KeyValuePair<String,String> kvp, PrintWriter pw) {
		pw.println(kvp.getKey() + " " + kvp.getValue());
	}
	@Override
	public void run() {
		System.out.println("Running parititioner " + mm.taskName + " with ID " + mm.taskID);
		try (FileReader fr = new FileReader(mm.inputFileArray.get(0));
				BufferedReader br = new BufferedReader(fr);)
			{
				ArrayList<PrintWriter> pwl = new ArrayList<PrintWriter>();
				// Create PrintWriters for each output file in master message
				for (String outputFile : mm.outputFileArray) {
					pwl.add(new PrintWriter(outputFile, "UTF-8"));
				}
				int numOutputFiles = pwl.size();
			String line = null;
			
			/*
			 * Loop over lines in input file and assign each key-value pair to a partition based on 
			 * partition function. Then write key-value pair to output file of that partition.
			 */
			while ((line = br.readLine()) != null) {
				String keyValueArray[] = line.split("\\s");
				KeyValuePair<String,String> kvp = new KeyValuePair<String,String>(keyValueArray[0], keyValueArray[1]);
				int partitionIndex = partition(kvp, numOutputFiles);
				emit(kvp, pwl.get(partitionIndex));
			}
			for (PrintWriter pw : pwl) {
				pw.close();
			}
			System.out.println("Partitioner " + mm.taskName + " with ID " + mm.taskID + " finished execution");
			sendCompletionMessage();
		} catch (FileNotFoundException e) {
			System.out.println("Partitioner " + mm.taskID + " cannot open file ");
		}
		catch (IOException e) {
			System.out.println("Partitioner " + mm.taskID + " encountered IO error ");
		}
	}
	
	/*
	 * Tell master that partition task is completed
	 */
	private void sendCompletionMessage() {
		MasterMessage cm = new MasterMessage(mm);
		try (Socket soc = new Socket(WorkerConfig.getMasterHostName(), WorkerConfig.getMasterPort());
				ObjectOutputStream oo = new ObjectOutputStream(soc.getOutputStream());) {
				oo.writeObject(cm);
				System.out.println("Partitioner " + mm.taskID + " sent task completion message to master");
		} catch (IOException e) {
			System.out.println("Partitioner " + mm.taskID + " failed to send task completion message to master");
		}
	}
}