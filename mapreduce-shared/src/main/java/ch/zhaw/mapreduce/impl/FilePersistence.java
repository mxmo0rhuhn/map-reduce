package ch.zhaw.mapreduce.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;

/**
 * Persistenz für Dateisysteme. Die (Zwischen-) Resultate werden in eine Dateigeschrieben.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class FilePersistence implements Persistence {

	@Inject
	private Logger logger;

	/**
	 * Das Verzeichnis, in welches alles geschrieben wird.
	 */
	private final String directory;

	/**
	 * Datei-Erweiterung für alle gespeicherten Dateien.
	 */
	private final String ending;

	@Inject
	FilePersistence(@Named("filepersistence.directory") String directory, @Named("filepersistence.ending") String ending) {
		this.directory = directory;
		this.ending = ending;
	}

	@PostConstruct
	public void checkIfDirectoryIsWritable() {
		File f = new File(directory);
		try {
			if (f.exists()) {
				logger.log(Level.FINER, "Directory " + directory + " exists");
			} else {
				f.mkdirs();
				logger.log(Level.FINER, "Directory " + directory + " for file persistence created");
			}
			if (!f.canWrite()) {
				logger.log(Level.SEVERE, "Directory " + directory + " is not writable");
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not create directory for persistence", e);
		}
	}

	/**
	 * Erstellt eine neue Datei im vorgegebenen Verzeichnis für die spezifizierte MapReduceID und InputID mit der
	 * vorgegebenen Erweiterung.
	 */
	private File createFile(String mrUuid, String inputUuid) {
		return new File(new File(directory), mrUuid + inputUuid + ending);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void storeMap(String mrUuid, String inputUuid, String key, String value) {
		File f = createFile(mrUuid, inputUuid);

		List<KeyValuePair> existingValues = new ArrayList<KeyValuePair>(getMap(mrUuid,
				inputUuid));
		existingValues.add(new KeyValuePair<String, String>(key, value));

		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(f));
			oos.writeObject(existingValues);
			logger.log(Level.FINEST, "Written to storage file " + f.getAbsolutePath());
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed to write to storage file " + f.getAbsolutePath(), e);
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void storeReduce(String mrUuid, String inputUuid, String result) {
		File f = createFile(mrUuid, inputUuid);

		List<String> existingValues = new ArrayList<String>(getReduce(mrUuid, inputUuid));
		existingValues.add(result);

		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(f));
			oos.writeObject(existingValues);
			logger.log(Level.FINEST, "Written to storage file " + f.getAbsolutePath());
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed to write to storage file " + f.getAbsolutePath(), e);
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMap(String mrUuid, String inputUuid) {
		File f = createFile(mrUuid, inputUuid);
		if (!f.exists()) {
			logger.finest("Storage file doesn't exist " + f.getAbsolutePath());
			return Collections.emptyList();
		}

		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(f));
			return (List<KeyValuePair>) ois.readObject();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to read storage file " + f.getAbsolutePath(), e);
			return Collections.emptyList();
		} finally {
			if (ois != null) {
				try {
					ois.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getReduce(String mrUuid, String inputUuid) {
		File f = createFile(mrUuid, inputUuid);
		if (!f.exists()) {
			logger.finest("Storage file doesn't exist " + f.getAbsolutePath());
			return Collections.emptyList();
		}

		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(f));
			return (List<String>) ois.readObject();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to read storage file " + f.getAbsolutePath(), e);
			return Collections.emptyList();
		} finally {
			if (ois != null) {
				try {
					ois.close();
				} catch (Exception ignore) {
				}
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void replaceMap(String mrUuid, String inputUuid, List<KeyValuePair> afterCombining)
			throws IllegalArgumentException {
		File f = createFile(mrUuid, inputUuid);

		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(f));
			oos.writeObject(afterCombining);
			logger.log(Level.FINEST, "Written to storage file " + f.getAbsolutePath());
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed to write to storage file " + f.getAbsolutePath(), e);
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy(String mrUuid, String taskUuid) {
		File file = createFile(mrUuid, taskUuid);
		if (file.delete()) {
			logger.finest("Successfully deleted " + file.getAbsolutePath());
		} else {
			logger.severe("Failed to delete " + file.getAbsolutePath());
		}
	}

	@Override
	public void destroy(String mapReduceTaskUuid) {
		// TODO for now we're just filling up the disk
	}

}
