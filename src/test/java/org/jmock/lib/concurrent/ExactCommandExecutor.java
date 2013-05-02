package org.jmock.lib.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of a thread-safe {@link Executor} that runs an exact number of commands, that is, it expects that a
 * predefined number of commands are supplied to it. Executing that number of commands is also the termination criteria.<br />
 * <br />
 * Only supplying a command to this executor will not run them. In order to run commands, the method
 * {@link #waitForExpectedTasks()} has to be invoked.<br />
 * <br />
 * One difference between this Executor and {@link DeterministicExecutor} is that if you have one thread executing
 * commands with this executor and you're waiting in the JUnit test for all commands to be executed. What could happen,
 * however, is that the runUntilIdle method returns before the other command were even queued to be executed. This
 * executor solves exactly this problem: it makes sure all expected commands get executed.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class ExactCommandExecutor implements ExecutorService {

	/**
	 * Commands to be executed
	 */
	/*
	 * we do not want to use the capacity restriction of the deque to limit the number of commands for the simple reason
	 * that all the commands to be run do not have to be in the deque at the same time.
	 */
	private final BlockingDeque<Runnable> commands = new LinkedBlockingDeque<Runnable>();

	/**
	 * Number of commands that have been run already
	 */
	private final AtomicInteger executedCommands = new AtomicInteger();

	/**
	 * Number of commands that have been scheduled for execution. May have run already or are still in the deque.
	 */
	private final AtomicInteger scheduledCommands = new AtomicInteger();

	/**
	 * Exact number of commands we want to run.
	 */
	private final int expectedCommands;

	/**
	 * Create a new ExactCommandExecutor for the specified number of commands
	 * 
	 * @param expectedCommands
	 *            how many commands we want to run before terminating
	 */
	public ExactCommandExecutor(int expectedCommands) {
		this.expectedCommands = expectedCommands;
	}

	/**
	 * Wait (potentially infinitely) for the specified amount of commands to be executed. This method is blocking, which
	 * means that unless the expected amount of commands has been run, it will not return.
	 * 
	 * @return true if all commands were run
	 */
	public boolean waitForExpectedTasks() {
		return waitForExpectedTasks(Long.MAX_VALUE, TimeUnit.SECONDS);
	}

	/**
	 * Tries to run all commands until the specified number of commands has been run. This method is blocking, that
	 * means, it does not return until the specified amount of commands could be run.
	 * 
	 * Interruption may be used for abnormal termination.
	 * 
	 * @param timeout
	 *            maximum wait
	 * @param unit
	 *            TimeUnit for maximum wait
	 * @return true if all commands were run withing the given timeout. false otherwise.
	 */
	public boolean waitForExpectedTasks(long timeout, TimeUnit unit) {
		while (executedCommands.get() < expectedCommands) {
			Runnable runnable;
			try {
				runnable = commands.poll(timeout, unit);
				if (runnable == null) {
					return false; // timeout exceeded
				}
			} catch (InterruptedException e) {
				return false; // may be used for abnormal termination
			}
			runnable.run();
			executedCommands.incrementAndGet();
		}
		return true;
	}

	/**
	 * Enqueues a new command for execution. Only supplying it here, does not make it run. Only the method {
	 * {@link #waitForExpectedTasks()} will start executing the commands.
	 * 
	 * @param command
	 *            the command to be run
	 */
	@Override
	public void execute(Runnable command) {
		if (scheduledCommands.incrementAndGet() > expectedCommands) {
			throw new IllegalStateException("Trying to execute too many commands");
		}
		if (!commands.add(command)) {
			throw new IllegalStateException("Cannot accept more work");
		}
	}

	@Override
	public void shutdown() {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public List<Runnable> shutdownNow() {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public boolean isShutdown() {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public boolean isTerminated() {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public <T> Future<T> submit(final Callable<T> task) {
		execute(new Runnable() {
			@Override
			public void run() {
				try {
					task.call();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		});
		return new DummyFuture<T>();
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public Future<?> submit(Runnable task) {
		return null;
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		throw new UnsupportedOperationException("not supported");
	}
}

class DummyFuture<T> implements Future<T> {

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public boolean isCancelled() {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public boolean isDone() {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		throw new UnsupportedOperationException("not supported");
	}

}