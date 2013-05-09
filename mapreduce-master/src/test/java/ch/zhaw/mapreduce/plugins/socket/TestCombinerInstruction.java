package ch.zhaw.mapreduce.plugins.socket;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.KeyValuePair;

public class TestCombinerInstruction implements CombinerInstruction {

	@Override
	public List<KeyValuePair> combine(Iterator<KeyValuePair> toCombine) {
		return Collections.emptyList();
	}

}
