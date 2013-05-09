package ch.zhaw.mapreduce.plugins.socket.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;
import ch.zhaw.mapreduce.plugins.socket.AgentTask;
import ch.zhaw.mapreduce.plugins.socket.AgentTaskFactory;

public class AgentTaskFactoryImpl implements AgentTaskFactory {

	private static final int BUF_SIZE = 256;

	private static final Logger LOG = Logger.getLogger(AgentTaskFactoryImpl.class.getName());

	@Override
	public AgentTask createAgentTask(WorkerTask workerTask) {
		if (workerTask instanceof MapWorkerTask) {
			return createMapAgentTask((MapWorkerTask) workerTask);
		} else if (workerTask instanceof ReduceWorkerTask) {
			return createReduceAgentTask((ReduceWorkerTask) workerTask);
		} else {
			throw new IllegalArgumentException("Unknown Type of WorkerTask: " + workerTask);
		}
	}

	/**
	 * Erstellt neuen MapAgentTask basierend auf dem MapWorkerTask
	 */
	private AgentTask createMapAgentTask(MapWorkerTask mwt) {
		return new MapAgentTask(mwt.getMapReduceTaskUuid(), mwt.getTaskUuid(), name(mwt.getMapInstruction()),
				bytes(mwt.getMapInstruction()),
				mwt.getCombinerInstruction() != null ? name(mwt.getCombinerInstruction()) : null,
				mwt.getCombinerInstruction() != null ? bytes(mwt.getCombinerInstruction()) : null, mwt.getInput());
	}

	/**
	 * Erstellt neuen ReduceAgentTask basierend auf dem ReduceWorkerTask
	 */
	private AgentTask createReduceAgentTask(ReduceWorkerTask rwt) {
		return new ReduceAgentTask(rwt.getMapReduceTaskUuid(), rwt.getTaskUuid(), name(rwt.getReduceInstruction()),
				bytes(rwt.getReduceInstruction()), rwt.getInput(), rwt.getValues());
	}

	/**
	 * Liest die bytes aus der Klasse, die hier als Instanz uebergeben wurde. Die bytes werden dann an den Client
	 * gesendet, der daraus die Instanz rekonstruiereren kann und so den Code ausführen. Dies setzt natürlich voraus,
	 * dass die Klasse als .class-File auf dem Dateisystem verfügbar ist und vom Klassenlader (der die Instanz geladen
	 * hat) gesehen wird.
	 * 
	 * @param instance
	 *            Instanz der zu serialisierenden Klasse
	 * @return bytes der übergebenen Instanz
	 * @throws IllegalArgumentException
	 *             wenn die klasse eine innere, member oder local klasse ist
	 * @throws IllegalArgumentException
	 *             wenn die klasse nicht als resouce gelesen werden kann
	 */
	static byte[] bytes(Object instance) {
		Class<?> klass = instance.getClass();
		if (klass.isAnonymousClass() || klass.isLocalClass() || klass.isMemberClass()) {
			throw new IllegalArgumentException("Only regular Top-Level Classes are allowed for now");
		}
		String resourceName = klass.getName().replace('.', '/') + ".class";
		InputStream is = klass.getClassLoader().getResourceAsStream(resourceName);
		if (is == null) {
			throw new IllegalArgumentException("ResouceNotFound: " + resourceName);
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] buf = new byte[BUF_SIZE];
		int read;
		try {
			while ((read = is.read(buf)) != -1) {
				bos.write(buf, 0, read);
			}
		} catch (Exception e) {
			LOG.severe("Failed to read Resouce " + resourceName + ": " + e.getMessage());
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException ignored) {
				}
			}
		}
		return bos.toByteArray();
	}

	/**
	 * Gibt den Namen der Klasse eine Instanz.
	 * 
	 * @param instance
	 * @return Name der Klasse dieser Instanz
	 */
	static String name(Object instance) {
		return instance.getClass().getName();
	}

}
