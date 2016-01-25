package worker;
import messages.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/*
 * Abstract standard base reducer class that is extended by application programmer to create custom reducers.
 * Reducer receives multiple input files and outputs one file.
 */
public abstract class ReducerBase implements Runnable {
	
	private MasterMessage mm;
	
	public ReducerBase(MasterMessage mm) {
		this.mm = mm;
	}
	/*
	 * Abstract reduce method that is to be defined in extended custom reducer class
	 */
	protected abstract KeyValuePair<String,String> reduce(KeyValueListPair<String, String> kvlp);
	
	/*
	 * Method that writes results to output file
	 */
	private void emit(KeyValuePair<String, String> kvp, PrintWriter pw) {
		pw.println(kvp.getKey() + " " + kvp.getValue());
	}
	
	@Override
	public void run() {
		System.out.println("Running reducer " + mm.taskName + " with ID " + mm.taskID);
		PrintWriter pw = null;
		try {
			// Create output file
			pw = new PrintWriter(mm.outputFileArray.get(0), "UTF-8");
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			System.out.println("Reducer " + mm.jobID + " encountered error while creating file");
		}
		
		/*
		 * Create an array list of key value pairs to aggregate all key value pairs
		 * from all input files and later sort them by key
		 */
		ArrayList<KeyValuePair<String,String>> kvpList = new ArrayList<KeyValuePair<String,String>> ();
		String line = null;
		for (String inputFile : mm.inputFileArray) {	// Loop over input files
			try (FileReader fr = new FileReader(inputFile);
					BufferedReader br = new BufferedReader(fr);) 
				{
				// Loop over lines in input file adding key value pairs to array list
				while ((line = br.readLine()) != null) {	
					String keyValueArray[] = line.split("\\s");
					KeyValuePair<String,String> kvp = new KeyValuePair<String,String>(keyValueArray[0], keyValueArray[1]);
					kvpList.add(kvp);
					}
				} catch (IOException e) {
					System.out.println("Reducer " + mm.jobID + " encountered IO error with " + inputFile);
				}
		}

		// Sort key value pairs in array list by key
		Collections.sort(kvpList, new Comparator<KeyValuePair<String,String>>() {
			@Override
			public int compare(KeyValuePair<String, String> o1,
					KeyValuePair<String, String> o2) {
				return (Integer.compare(Integer.parseInt(o1.getKey()), Integer.parseInt(o2.getKey())));
			}
        });
		
		/*
		 * Loop over sorted key value pairs in array list and create a new list 
		 * of key-value list pairs. (Key, list of values for that key). Call reduce with
		 * each key-value list and reduce all values for one key into one value. Write data
		 * to output file.
		 */
		Iterator<KeyValuePair<String, String>> itr = kvpList.iterator();
		KeyValuePair<String, String> kvp = itr.next();
		KeyValueListPair<String, String> kvlp = new KeyValueListPair<String, String>(kvp.getKey());
		kvlp.addValue(kvp.getValue());
		KeyValuePair<String, String> nextKvp = null;
		while (itr.hasNext()) {
			nextKvp = itr.next();
			if (nextKvp.getKey().equals(kvlp.getKey())) {
				kvlp.addValue(nextKvp.getValue());
			} else {
				// reduce and output
				emit(reduce(kvlp), pw);	
				kvlp = new KeyValueListPair<String, String>(nextKvp.getKey());
				kvlp.addValue(nextKvp.getValue());
			}
		}
		// reduce and output final key-value list pair that was left at the end of the previous loop
		emit(reduce(kvlp), pw);
		pw.close();
		System.out.println("Reducer " + mm.taskName + " with ID " + mm.taskID + " finished execution");
		sendCompletionMessage();
	}
	
	/*
	 * Tell master this reduce task is completed
	 */
	private void sendCompletionMessage() {
		MasterMessage cm = new MasterMessage(mm);
		try (Socket soc = new Socket(WorkerConfig.getMasterHostName(), WorkerConfig.getMasterPort());
				ObjectOutputStream oo = new ObjectOutputStream(soc.getOutputStream());) {
				oo.writeObject(cm);
				System.out.println("Reducer " + mm.taskID + " sent task completion message to master");
		} catch (IOException e) {
			System.out.println("Reducer " + mm.taskID + " failed to send task completion message to master");
		}
	}
}