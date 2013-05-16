package ch.zhaw.mapreduce.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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

	private static final Logger LOG = Logger.getLogger(FilePersistence.class.getName());

	private static final Random seed = new Random();

	private final File baseDir;

	@Inject
	FilePersistence(@Named("filepersistence.directory") String directory) {
		this.baseDir = new File(directory, Integer.toString(seed.nextInt() + (int) System.currentTimeMillis()));
	}

	@PostConstruct
	public void initDirectory() {
		if (!this.baseDir.exists()) {
			if (!this.baseDir.mkdirs()) {
				throw new IllegalArgumentException(baseDir.getAbsolutePath() + " does not exist and cannot be created");
			} else {
				LOG.log(Level.FINE, "Directory {0} created", baseDir.getAbsolutePath());
			}
		}
		if (!this.baseDir.canWrite()) {
			throw new IllegalArgumentException(baseDir.getAbsolutePath() + " is not writable");
		}
		LOG.log(Level.INFO, "Using directory {0} for persistence", baseDir.getAbsolutePath());
	}

	/**
	 * Erstellt eine neue Datei im vorgegebenen Verzeichnis für die spezifizierte MapReduceID und InputID mit der
	 * vorgegebenen Erweiterung.
	 */
	private File createFile(String inputUuid) {
		return new File(baseDir, inputUuid);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMapResults(String inputUuid) {
		File f = createFile(inputUuid);
		if (!f.exists()) {
			LOG.log(Level.FINEST, "Storage file doesn't exist {0}", f.getAbsolutePath());
			return Collections.emptyList();
		}

		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(f));
			return (List<KeyValuePair>) ois.readObject();
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Failed to read storage file", e);
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
	public List<String> getReduceResults(String inputUuid) {
		File f = createFile(inputUuid);
		if (!f.exists()) {
			LOG.log(Level.FINEST, "Storage file doesn't exist {0}", f.getAbsolutePath());
			return Collections.emptyList();
		}

		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(f));
			return (List<String>) ois.readObject();
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Failed to read storage file ", e);
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
	public boolean destroy(String taskUuid) {
		File file = createFile(taskUuid);
		if (file.delete()) {
			LOG.log(Level.FINEST, "Successfully deleted {0}", file.getAbsolutePath());
			return true;
		} else {
			LOG.log(Level.WARNING, "Failed to delete {0}", file.getAbsolutePath());
			return false;
		}
	}

	@Override
	public boolean storeReduceResults(String taskUuid, List<String> redRes) {
		File file = createFile(taskUuid);
		if (file.exists()) {
			LOG.log(Level.SEVERE, "File {0} for TaskUuid {1} already exists", new Object[] { file.getAbsolutePath(),
					taskUuid });
			return false;
		}

		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(file));
			oos.writeObject(redRes);
			LOG.log(Level.FINEST, "Written to storage file {0}", file.getAbsolutePath());
			return true;
		} catch (IOException e) {
			LOG.log(Level.SEVERE, "Failed to write to storage file ", e);
			return false;
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	@Override
	public boolean storeMapResults(String taskUuid, List<KeyValuePair> mapRes) {
		File file = createFile(taskUuid);
		if (file.exists()) {
			LOG.log(Level.SEVERE, "File {0} for TaskUuid {1} already exists", new Object[] { file.getAbsolutePath(),
					taskUuid });
			return false;
		}

		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(file));
			oos.writeObject(mapRes);
			LOG.log(Level.FINEST, "Written to storage file {0}", file.getAbsolutePath());
			return true;
		} catch (IOException e) {
			LOG.log(Level.SEVERE, "Failed to write to storage file ", e);
			return false;
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (Exception ignore) {
				}
			}
		}
	}
}
