package ch.zhaw.mapreduce.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

	private final File mapBaseDir;

	private final File reduceBaseDir;

	@Inject
	FilePersistence(@Named("filepersistence.directory") String directory) {
		String distinctor = Long.toString(System.currentTimeMillis()) + seed.nextInt(Integer.MAX_VALUE);
		this.mapBaseDir = new File(directory, distinctor + "map");
		this.reduceBaseDir = new File(directory, distinctor + "red");
	}

	@PostConstruct
	public void initDirectories() {
		createAndWritable(mapBaseDir);
		createAndWritable(reduceBaseDir);
		LOG.log(Level.INFO, "Using directory {0} and {1} for persistence", new Object[] { mapBaseDir.getAbsolutePath(),
				reduceBaseDir.getAbsolutePath() });
	}

	private void createAndWritable(File dir) {
		if (dir.exists()) {
			throw new IllegalStateException("Directory must not have existed before: " + dir.getAbsolutePath());
		}
		if (!dir.mkdirs()) {
			throw new IllegalArgumentException(dir.getAbsolutePath() + " does not exist and cannot be created");
		} else {
			LOG.log(Level.FINE, "Directory {0} created", dir.getAbsolutePath());
		}
		if (!dir.canWrite()) {
			throw new IllegalArgumentException(dir.getAbsolutePath() + " is not writable");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMapResults() {
		File[] files = this.mapBaseDir.listFiles();
		if (files == null || files.length == 0) {
			LOG.log(Level.FINEST, "No results found in {0}", this.mapBaseDir.getAbsolutePath());
			return Collections.emptyList();
		}

		List<KeyValuePair> results = new LinkedList<KeyValuePair>();
		for (File file : files) {
			ObjectInputStream ois = null;
			try {
				ois = new ObjectInputStream(new FileInputStream(file));
				results.addAll((List<KeyValuePair>) ois.readObject());
			} catch (Exception e) {
				LOG.log(Level.SEVERE, "Failed to read storage file ", e);
			} finally {
				if (ois != null) {
					try {
						ois.close();
					} catch (Exception ignore) {
					}
				}
			}

		}
		return results;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, List<String>> getReduceResults() {
		File[] files = this.reduceBaseDir.listFiles();
		if (files == null || files.length == 0) {
			LOG.log(Level.FINEST, "No results found in {0}", this.reduceBaseDir.getAbsolutePath());
			return Collections.emptyMap();
		}

		Map<String, List<String>> results = new HashMap<String, List<String>>();
		for (File file : files) {
			ObjectInputStream ois = null;
			try {
				ois = new ObjectInputStream(new FileInputStream(file));
				ReduceResult res = (ReduceResult) ois.readObject();
				if (results.put(res.key, res.values) != null) {
					LOG.log(Level.SEVERE, "Stored dupicated Reduce Results for Key = {0}", res.key);
				}
			} catch (Exception e) {
				LOG.log(Level.SEVERE, "Failed to read storage file ", e);
			} finally {
				if (ois != null) {
					try {
						ois.close();
					} catch (Exception ignore) {
					}
				}
			}

		}
		return results;
	}

	@Override
	public boolean destroyMap(String taskUuid) {
		File file = new File(this.mapBaseDir, taskUuid);
		if (file.delete()) {
			LOG.log(Level.FINEST, "Successfully deleted {0}", file.getAbsolutePath());
			return true;
		} else {
			LOG.log(Level.WARNING, "Failed to delete {0}", file.getAbsolutePath());
			return false;
		}
	}

	@Override
	public boolean destroyReduce(String taskUuid) {
		File file = new File(this.reduceBaseDir, taskUuid);
		if (file.delete()) {
			LOG.log(Level.FINEST, "Successfully deleted {0}", file.getAbsolutePath());
			return true;
		} else {
			LOG.log(Level.WARNING, "Failed to delete {0}", file.getAbsolutePath());
			return false;
		}
	}

	@Override
	public boolean storeReduceResults(String taskUuid, String key, List<String> redRes) {
		File file = new File(this.reduceBaseDir, taskUuid);
		if (file.exists()) {
			LOG.log(Level.SEVERE, "File {0} for TaskUuid {1} already exists", new Object[] { file.getAbsolutePath(),
					taskUuid });
			return false;
		}

		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(file));
			oos.writeObject(new ReduceResult(key, redRes));
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
		File file = new File(this.mapBaseDir, taskUuid);
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

	@Override
	public boolean suicide() {
		LOG.entering(getClass().getName(), "suicide");
		boolean success = true;
		if (!deletedir(this.mapBaseDir)) {
			LOG.log(Level.SEVERE, "Failed to delete {0}", mapBaseDir.getAbsolutePath());
			success = false;
		}
		if (!deletedir(this.reduceBaseDir)) {
			LOG.log(Level.SEVERE, "Failed to delete {0}", reduceBaseDir.getAbsolutePath());
			success = false;
		}

		LOG.exiting(getClass().getName(), "suicide", success);
		return success;
	}
	
	/**
	 * Löscht ein Verzeichnis rekursiv, da die Standard-Java-Methode nur leere Verzeichnisse löschen kann.
	 * @return true, wenn das Verzeichnis gelöscht werden konnte, sonst false
	 */
	static boolean deletedir(File f) {
		if (f.isDirectory()) {
			for (File kids : f.listFiles()) {
				if (!deletedir(kids)) {
					return false;
				}
			}
		}
		return f.delete();
	}

	/**
	 * Diese Klasse wird ausschliesslich zum speichern der Reduce-Resultate verwenet und ist gegen aussen nicht sichtbar.
	 * @author Reto Hablützel (rethab)
	 *
	 */
	private static class ReduceResult implements Serializable {
		private static final long serialVersionUID = 1364848029826475659L;
		final String key;
		final List<String> values;

		ReduceResult(String key, List<String> values) {
			this.key = key;
			this.values = values;
		}
	}
}
