package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.plugins.socket.AgentTask;
import ch.zhaw.mapreduce.plugins.socket.InvalidAgentTaskException;
import ch.zhaw.mapreduce.plugins.socket.SocketAgent;
import ch.zhaw.mapreduce.plugins.socket.SocketTaskResult;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;
import ch.zhaw.mapreduce.plugins.socket.TaskRunnerFactory;

import com.google.inject.assistedinject.Assisted;

import de.root1.simon.annotation.SimonRemote;

/**
 * Der Socket Agent ist quasi der Client-Seitige Worker. Er führt Tasks aus und gibt das Resultat zurück an den Master.
 * Der SocketAdapter ist somit der verbindende Teil zwischen dem Server und Client von der Client-Seite. Er wird bei der
 * initialen Registrierung auf den Server gesandt und als Callback wird ein Task damit ausgeführt.
 * 
 * Diese Klasse muss die Annotation '@SimonRemote' haben, weil sie über den Socket geschickt wird.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
@SimonRemote(SocketAgent.class)
public class SocketAgentImpl implements SocketAgent {

	private static final Logger LOG = Logger.getLogger(SocketAgentImpl.class.getName());

	/** IP - Adresse von diesem Client/Worker */
	private final String clientIp;

	private final TaskRunnerFactory trFactory;

	@Inject
	SocketAgentImpl(@Assisted String clientIp, TaskRunnerFactory trFactory) {
		this.clientIp = clientIp;
		this.trFactory = trFactory;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void helloslave() {
		LOG.info("Successfully registered on Master");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SocketTaskResult runTask(AgentTask task) throws InvalidAgentTaskException {
		String mrUuid = task.getMapReduceTaskUuid();
		String taskUuid = task.getTaskUuid();

		LOG.info("ENTER: SocketAgentImpl.runTask: " + mrUuid + " " + taskUuid);
		TaskRunner runner = this.trFactory.createTaskRunner(task);
		try {
			return runner.runTask();
		} finally {
			LOG.info("EXIT: SocketAgentImpl.runTask: " + mrUuid + " " + taskUuid);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getIp() {
		return this.clientIp;
	}
}
