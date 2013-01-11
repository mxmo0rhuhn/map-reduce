package ch.zhaw.mapreduce.registry;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.google.inject.BindingAnnotation;

/**
 * Annotation für spezifischere Bindings. Mit dieser Guice Annotation kann ein Binding eines Interface weiter
 * eingeschränkt werden. z.B. Gibt es das Interface ExecutorService und in manchen Fällen ist man sich sicher, dass man
 * einen SingleThreaded ExecutorService will. So kann dann ein ExecutorService speziell gebindet werden, wenn er mit
 * SingleThreaded annotiert ist.
 *  
 * Diese Annotation ist spezifisch fuer den Executor im Pool.
 * 
 * @author Reto
 * 
 * @see MapReduceConfig#createPoolExecutor()
 * 
 */
@BindingAnnotation
@Target({ ElementType.PARAMETER, ElementType.METHOD })
@Retention(RUNTIME)
public @interface PoolExecutor {

}
