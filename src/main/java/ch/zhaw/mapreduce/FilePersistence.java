package ch.zhaw.mapreduce;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class FilePersistence implements Persistence {
	
	private final String path = System.getProperty("java.io.tmpdir") + "/mapreduce/tempResults/";
	private final String fileEnding = ".ser";
	
	@Override
	public void store(String mrUuid, String inputUuid, String key, String value) {
		List<KeyValuePair> toFile = get(mrUuid, inputUuid);
		File storageFile = new File(path+mrUuid+inputUuid+fileEnding);

		toFile.add(new KeyValuePair(key, value));
		
		 try {
			new ObjectOutputStream(new FileOutputStream(storageFile)).writeObject(toFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void store(String mrUuid, String inputUuid, String result) {

		List<String> toFile = null; // TODO 
		File storageFile = new File(path+mrUuid+inputUuid+fileEnding);
		
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(storageFile));
			toFile = (List<String>) ois.readObject();
		} catch (Exception e) {
			toFile = new ArrayList<String>();
		}
		toFile.add(result);
		
		 try {
			new ObjectOutputStream(new FileOutputStream(storageFile)).writeObject(toFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public List<KeyValuePair> get(String mrUuid, String inputUuid) {
		List<KeyValuePair> toFile;
		try {
			// TODO implement
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(""));
			toFile = (List<KeyValuePair>) ois.readObject();
		} catch (Exception e) {
			toFile = new ArrayList<KeyValuePair>();
		}
		return toFile;
	}

	@Override
	public void replace(String mrUuid, String inputUuid, List<KeyValuePair> afterCombining) {
		// TODO Auto-generated method stub

	}

	@Override
	public void destroy(String mrUuid, String inputUuid) {
		// TODO Auto-generated method stub

	}
	private ObjectInputStream getFile(String mrUuid, String inputUuid) throws IOException {
		return null;
	}
}
