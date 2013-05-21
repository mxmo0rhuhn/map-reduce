package ch.zhaw.mapreduce.plugins.socket.interceptors;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

/**
 * Simpler Interceptor, der die Ausführungszeit einer Methode misst.
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public class ExecutionTimeInterceptor implements MethodInterceptor {

	private final static Logger LOG = Logger.getLogger(ExecutionTimeInterceptor.class.getName());

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		long start = System.currentTimeMillis();
		try {
			return invocation.proceed();
		} finally {
			long executionTime = System.currentTimeMillis() - start;
			LOG.log(Level.FINE, "Execution Time: {0}ms", executionTime);
		}
	}

}
