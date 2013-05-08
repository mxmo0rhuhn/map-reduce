package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.plugins.socket.SocketTaskResult;
import ch.zhaw.mapreduce.plugins.socket.SocketTaskResultFactory;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;

import com.google.inject.assistedinject.Assisted;

/**
 * Führt eine ReduceInstruction mit ihren Eingabewerten aus.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public final class ReduceTaskRunner implements TaskRunner {

	private static final Logger LOG = Logger.getLogger(ReduceTaskRunner.class.getName());

	private final String mapReduceTaskUuid;

	private final String taskUuid;

	private final ReduceInstruction redInstr;

	private final String key;

	private final List<KeyValuePair> values;

	private final ContextFactory ctxFactory;

	private final SocketTaskResultFactory strFactory;

	@Inject
	ReduceTaskRunner(@Assisted("mapReduceTaskUuid") String mapReduceTaskUuid, @Assisted("taskUuid") String taskUuid,
			@Assisted ReduceInstruction redInstr, @Assisted("key") String key, @Assisted List<KeyValuePair> values,
			ContextFactory ctxFactory, SocketTaskResultFactory strFactory) {
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.taskUuid = taskUuid;
		this.redInstr = redInstr;
		this.key = key;
		this.values = values;
		this.ctxFactory = ctxFactory;
		this.strFactory = strFactory;
	}

	/**
	 * Führt die ReduceInstruction aus und gibt die Resultate in der korrekten Struktur zurück.
	 */
	@Override
	public SocketTaskResult runTask() {
		LOG.entering(getClass().getName(), "runTask");
		Context ctx = this.ctxFactory.createContext(this.mapReduceTaskUuid, this.taskUuid);
		try {
			this.redInstr.reduce(ctx, this.key, this.values.iterator());
			return this.strFactory.createSuccessResult(ctx.getReduceResult());
		} catch (Exception e) {
			return this.strFactory.createFailureResult(e);
		} finally {
			LOG.exiting(getClass().getName(), "runTask");
		}
	}

}
