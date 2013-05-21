package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.plugins.socket.TaskResult;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;

import com.google.inject.assistedinject.Assisted;

/**
 * Führt eine ReduceInstruction mit ihren Eingabewerten aus.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class ReduceTaskRunner implements TaskRunner {

	private static final Logger LOG = Logger.getLogger(ReduceTaskRunner.class.getName());

	private final String taskUuid;

	private final ReduceInstruction redInstr;

	private final String key;

	private final List<KeyValuePair> values;

	private final Provider<Context> ctxProvider;

	@Inject
	ReduceTaskRunner(@Assisted("taskUuid") String taskUuid,
			@Assisted ReduceInstruction redInstr, @Assisted("key") String key, @Assisted List<KeyValuePair> values,
			Provider<Context> ctxProvider) {
		this.taskUuid = taskUuid;
		this.redInstr = redInstr;
		this.key = key;
		this.values = values;
		this.ctxProvider = ctxProvider;
	}

	/**
	 * Führt die ReduceInstruction aus und gibt die Resultate in der korrekten Struktur zurück.
	 */
	@Override
	public TaskResult runTask() {
		LOG.entering(getClass().getName(), "runTask");
		Context ctx = this.ctxProvider.get();
		try {
			this.redInstr.reduce(ctx, this.key, this.values.iterator());
			return new ReduceTaskResult(this.taskUuid, ctx.getReduceResult());
		} catch (Exception e) {
			return new ReduceTaskResult(this.taskUuid, e);
		} finally {
			LOG.exiting(getClass().getName(), "runTask");
		}
	}

}
