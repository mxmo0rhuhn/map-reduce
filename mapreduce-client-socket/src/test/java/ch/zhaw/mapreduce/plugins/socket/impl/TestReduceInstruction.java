package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.Iterator;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceEmitter;
import ch.zhaw.mapreduce.ReduceInstruction;

public class TestReduceInstruction implements ReduceInstruction {

	@Override
	public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {

	}

}
