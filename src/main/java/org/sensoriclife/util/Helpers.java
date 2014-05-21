package org.sensoriclife.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;

/**
 *
 * @author jnphilipp, paul
 * @version 0.5.0
 */
public class Helpers {
	/**
	 * Returns path to the user directory.
	 * @return path to user directory
	 */
	public static String getUserDir() {
		return System.getProperty("user.dir");
	}

	/**
	 * Tags the content with the tag name.
	 * @param tagname tag name
	 * @param content content
	 * @return tagged content
	 */
	public static String tag_with_content(String tagname, String content) {
		return "<" + tagname + ">" + content + "</" + tagname + ">";
	}

	/**
	 * Returns the first content of the given tag.
	 * @param tagname tag name
	 * @param toSearch string to search for tag
	 * @return first content of tag
	 */
	public static String get_tag_content_first(String tagname, String toSearch) {
		Matcher m = Pattern.compile("<" + tagname + ">(.+?)</" + tagname + ">").matcher(toSearch);

		if ( m.find() )
			return m.group(1);

		return null;
	}

	/**
	 * Joins the given array to a <code>String</code>, separated with the given cement.
	 * @param <T>
	 * @param array array
	 * @param cement cement
	 * @return joined string
	 */
	public static <T> String join(T[] array, String cement) {
		StringBuilder builder = new StringBuilder();

		if ( array == null || array.length == 0 )
			return null;

		for ( T t : array )
			builder.append(t).append(cement);

		builder.delete(builder.length() - cement.length(), builder.length());
		return builder.toString();
	}

	/**
	 * Joins the given collection to a <code>String</code>, separated with the given cement.
	 * @param <T>
	 * @param collection collection
	 * @param cement cement
	 * @return joined string
	 */
	public static <T> String join(Collection<T> collection, String cement) {
		StringBuilder builder = new StringBuilder();

		if ( collection == null || collection.isEmpty() )
			return null;

		for ( T t : collection )
			builder.append(t).append(cement);

		builder.delete(builder.length() - cement.length(), builder.length());
		return builder.toString();
	}

	/**
	 * Stores the file given by the URL to the path.
	 * @param url source URL
	 * @param path destination path
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static void saveURLToFile(String url, String path) throws MalformedURLException, IOException {
		InputStream in = URI.create(url).toURL().openStream();
		Files.copy(in, Paths.get(path));
	}

	/**
	 * Returns a MD5-Hash for the given text.
	 * @param text source for the MD5-Hash
	 * @return MD5-Hash
	 * @throws NoSuchAlgorithmException
	 */
	public static String getMD5(String text) throws NoSuchAlgorithmException {
		MessageDigest messageDigest = MessageDigest.getInstance("MD5");
		messageDigest.update(text.getBytes(Charset.forName("UTF8")));
		byte[] digest = messageDigest.digest();
		BigInteger bigInt = new BigInteger(1, digest);
		String hashtext = bigInt.toString(16);
		while ( hashtext.length() < 32 )
			hashtext = "0" + hashtext;

		return hashtext;
	}

	/**
	 * Returns a SHA-512-Hash for the given text.
	 * @param text source for the SHA-512-Hash
	 * @return SHA-512-Hash
	 * @throws NoSuchAlgorithmException
	 */
	public static String getSHA512(String text) throws NoSuchAlgorithmException {
		MessageDigest messageDigest = MessageDigest.getInstance("SHA-512");
		messageDigest.update(text.getBytes(Charset.forName("UTF8")));
		byte[] digest = messageDigest.digest();
		BigInteger bigInt = new BigInteger(1, digest);
		String hashtext = bigInt.toString(16);
		while ( hashtext.length() < 32 )
			hashtext = "0" + hashtext;

		return hashtext;
	}

	/**
	 * Returns a SHA-512-Hash for the given file.
	 * @param path to a file for the SHA-512-Hash
	 * @return SHA-512-Hash
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public static String getSHA512OfFile(String file) throws IOException, NoSuchAlgorithmException {
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
		byte[] bytes = IOUtils.toByteArray(bis);
		bis.close();

		MessageDigest messageDigest = MessageDigest.getInstance("SHA-512");
		messageDigest.update(bytes);
		byte[] digest = messageDigest.digest();
		BigInteger bigInt = new BigInteger(1, digest);
		String hashtext = bigInt.toString(16);
		while ( hashtext.length() < 32 )
			hashtext = "0" + hashtext;

		return hashtext;
	}

	/**
	 * Converts the given object to a byte array.
	 * @param obj object
	 * @return byte array
	 * @throws IOException 
	 */
	public static byte[] toByteArray(Object obj) throws IOException {
		byte[] byt = null;
		ByteArrayOutputStream bytout = null;
		ObjectOutputStream objout = null;

		try {
			bytout = new ByteArrayOutputStream();
			objout = new ObjectOutputStream(bytout);
			objout.writeObject(obj);
			objout.flush();
			byt = bytout.toByteArray();
		}
		finally {
			if ( objout != null )
				objout.close();

			if ( bytout != null )
				bytout.close();
		}

		return byt;
	}

	/**
	 * Converts the given byte array to an object.
	 * @param byt byte array
	 * @return Object
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 */
	public static Object toObject(byte[] byt) throws IOException, ClassNotFoundException {
		Object obj = null;
		ByteArrayInputStream bytin = null;
		ObjectInputStream objin = null;

		try {
			bytin = new ByteArrayInputStream(byt);
			objin = new ObjectInputStream(bytin);
			obj = objin.readObject();
		}
		finally {
			if ( bytin != null )
				bytin.close();

			if ( objin != null )
				objin.close();
		}

		return obj;
	}

	/**
	 * Deep copy the given object through a byte array.
	 * @param obj object to copy
	 * @return copy of the given object
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 */
	public static Object deepCopy(Object obj) throws IOException, ClassNotFoundException {
		return toObject(ArrayUtils.clone(toByteArray(obj)));
	}
}