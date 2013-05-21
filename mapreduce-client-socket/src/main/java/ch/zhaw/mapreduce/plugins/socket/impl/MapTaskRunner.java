package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.plugins.socket.TaskResult;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;

import com.google.inject.assistedinject.Assisted;

/**
 * Führt eine MapInstruction mit deren Input aus.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class MapTaskRunner implements TaskRunner {

	private static final Logger LOG = Logger.getLogger(MapTaskRunner.class.getName());

	private final String taskUuid;

	private final MapInstruction mapInstr;

	// optional. null wenn nicht vorhanden
	private final CombinerInstruction combInstr;

	private final String input;

	private final Provider<Context> ctxProvider;

	@Inject
	MapTaskRunner(@Assisted("taskUuid") String taskUuid,
			@Assisted MapInstruction mapInstr, @Assisted @Nullable CombinerInstruction combInstr,
			@Assisted("input") String input, Provider<Context> ctxProvider) {
		this.taskUuid = taskUuid;
		this.mapInstr = mapInstr;
		this.combInstr = combInstr;
		this.input = input;
		this.ctxProvider = ctxProvider;
	}

	/**
	 * Führt den Task aus und kombiniert die Resultate wenn eine Combiner-Instruction existiert.
	 */
	@Override
	public TaskResult runTask() {
		LOG.entering(MapTaskRunner.class.getName(), "runTask");
		// TODO ineffizient. impliziert das postconstructfeature, welches per reflection zeugs macht
		Context ctx = this.ctxProvider.get();
		try {
			// Mappen
			this.mapInstr.map(ctx, input);

			// Alle Ergebnisse verdichten.
			List<KeyValuePair> mapResult = ctx.getMapResult();
			if (this.combInstr != null) {
				mapResult = this.combInstr.combine(mapResult.iterator());
			}
			return new MapTaskResult(this.taskUuid, mapResult);
		} catch (Exception e) {
			return new MapTaskResult(this.taskUuid, e);
		} finally {
			LOG.exiting(MapTaskRunner.class.getName(), "runTask");
		}
	}
}
