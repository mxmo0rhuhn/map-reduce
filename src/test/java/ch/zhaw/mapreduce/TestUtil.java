package ch.zhaw.mapreduce;

import com.google.inject.Provider;

public final class TestUtil {

	public static <T> Provider<T> toProvider(final T instance) {
		return new Provider<T>() {
			@Override
			public T get() {
				return instance;
			}
		};
	}


}
