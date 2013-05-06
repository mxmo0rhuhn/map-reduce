package ch.zhaw.mapreduce.plugins.socket;

import java.io.Serializable;

import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import de.root1.simon.annotation.SimonRemote;

@SimonRemote(MapInstruction.class)
public class TestMapInstruction implements MapInstruction, Serializable {

	private static final long serialVersionUID = -5951237460480827687L;

	@Override
	public void map(MapEmitter emitter, String input) {
		emitter.emitIntermediateMapResult("key", input);
	}

}