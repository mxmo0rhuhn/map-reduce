package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.plugins.socket.AgentTask;
import ch.zhaw.mapreduce.plugins.socket.SocketAgent;

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

	/** Transient, weil die factory nur auf dem client benötigt wird. */
	private transient final ContextFactory ctxFactory;

	/** IP - Adresse von diesem Client/Worker */
	private final String clientIp;

	@Inject
	SocketAgentImpl(@Assisted String clientIp, ContextFactory ctxFactory) {
		this.clientIp = clientIp;
		this.ctxFactory = ctxFactory;
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
	public Object runTask(AgentTask task) {
		String mrUuid = task.getMapReduceTaskUuid();
		String taskUuid = task.getTaskUuid();
		LOG.info("New Task to be Run: " + mrUuid + " " + taskUuid);
		Context ctx = this.ctxFactory.createContext(mrUuid, taskUuid);
		// TODO how to tell the master we've failed?
		task.runTask(ctx);
		List<KeyValuePair> res = ctx.getMapResult();
		if (res != null) {
			return res;
		} else {
			return ctx.getReduceResult();
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
