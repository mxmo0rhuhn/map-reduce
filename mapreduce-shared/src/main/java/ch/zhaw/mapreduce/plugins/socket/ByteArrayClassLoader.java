package ch.zhaw.mapreduce.plugins.socket;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Diese Klasse erweitert die Standard-Java-Klasse ClassLoader um deren Methode defineClass public zu machen, damit wir
 * sie verwenden können.
 * 
 * Im weiteren Sinne wird diese Klasse dazu benutzt, basierend auf einem Byte-Array eine Klassendefinition zu laden.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class ByteArrayClassLoader extends ClassLoader {

	/**
	 * Die gleiche Klasse kann nicht zweimal geladen werden, also muessen wir sie cachen und ggf. die bereits geladene
	 * Klasse zurueckgeben.
	 */
	private final Map<String, Class<?>> cache = new HashMap<String, Class<?>>();

	/**
	 * lock fuer cache-map. der lock ist fair, weil der erste thread, der die instanz beanträgt, wird sie dann erstellen
	 * und die folgenden können diese benutzen. es macht daher keinen sinn, wenn einer den anderen 'überholt', weil sie
	 * sowieso beide das gleich ziel haben (nämlich die klasse zu laden).
	 */
	private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

	/**
	 * Lädt die Klasse mit dem Standard-Java Cloassloader.
	 * 
	 * @param className
	 *            Name der Implementations-Klasse
	 * @param bytes
	 *            Bytes der Definiton
	 * @return Instanz der Klassendefinition
	 */
	public Class<?> defineClass(String className, byte[] bytes) {
		Map<String, Class<?>> cache = this.cache;
		Lock readLock = this.rwLock.readLock();

		Class<?> klass;
		readLock.lock();
		try {
			klass = cache.get(className);
		} finally {
			readLock.unlock();
		}
		if (klass != null) {
			return klass;
		}

		Lock writeLock = this.rwLock.writeLock();
		writeLock.lock();
		try {
			klass = cache.get(className);
			if (klass == null) {
				klass = defineClass(className, bytes, 0, bytes.length, null);
				cache.put(className, klass);
				return klass;
			}
			return klass;
		} finally {
			writeLock.unlock();
		}
	}
}