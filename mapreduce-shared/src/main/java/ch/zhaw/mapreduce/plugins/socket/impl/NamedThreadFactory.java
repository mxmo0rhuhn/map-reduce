package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadFactory für Standard Java-Threads mit einem bestimmten Namen.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public final class NamedThreadFactory implements ThreadFactory {

	/**
	 * Basis Factory
	 */
	private final ThreadFactory factory;

	/**
	 * Counter, wird pro Thread erhöht
	 */
	private final AtomicInteger cnt = new AtomicInteger();

	/**
	 * Der Basis-Name. Hintendran kommt der Counter
	 */
	private final String baseName;

	/**
	 * Erstellt neue ThreadFactory mit Basis-Namen
	 */
	public NamedThreadFactory(String baseName) {
		this.factory = Executors.defaultThreadFactory();
		this.baseName = baseName + '-';
	}

	/**
	 * Erstellt neuen Thread für Runnable und setzt den Namen.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public Thread newThread(Runnable r) {
		Thread t = this.factory.newThread(r);
		t.setName(this.baseName + cnt.getAndIncrement());
		return t;
	}

}
