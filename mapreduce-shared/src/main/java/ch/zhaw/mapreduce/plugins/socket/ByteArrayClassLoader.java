package ch.zhaw.mapreduce.plugins.socket;

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
	 * Lädt die Klasse mit dem Standard-Java Cloassloader.
	 * 
	 * @param className
	 *            Name der Implementations-Klasse
	 * @param bytes
	 *            Bytes der Definiton
	 * @return Instanz der Klassendefinition
	 */
	public Class<?> defineClass(String className, byte[] bytes) {
		return defineClass(className, bytes, 0, bytes.length, null);
	}

}