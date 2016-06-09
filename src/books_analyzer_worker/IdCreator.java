package books_analyzer_worker;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class IdCreator {
	public static String create(String input) {
		MessageDigest mDigest;
		try {
			mDigest = MessageDigest.getInstance("SHA1");
			byte[] result = mDigest.digest(input.getBytes());
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < result.length; i++) {
				sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
			}
			return Long.valueOf((new BigInteger(sb.toString(), 16)).longValue()).toString();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return "0";
		}
	}
}
