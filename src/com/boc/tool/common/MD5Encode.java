package com.boc.tool.common;

import java.security.MessageDigest;




/**
 * �û�����ļ���

 * @author yuchunzu
 *
 */
public class MD5Encode {

	private MessageDigest md5;

	private static MD5Encode Instance;

	/**
	 * Constructs the MD5 object and sets the string whose MD5 is to be
	 * computed.
	 * 
	 * @param inStr
	 *            the <code>String</code> whose MD5 is to be computed
	 */
	public synchronized static MD5Encode getMD5() {
		if (Instance == null)
			Instance = new MD5Encode();
		return Instance;
	}

	private MD5Encode() {
		try {
			this.md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}

	/**
	 * Computes the MD5 fingerprint of a string.
	 * 
	 * @return the MD5 digest of the input <code>String</code>
	 */
	public String encode(String inStr) {
		// convert input String to a char[]
		// convert that char[] to byte[]
		// get the md5 digest as byte[]
		// bit-wise AND that byte[] with 0xff
		// prepend "0" to the output StringBuffer to make sure that we don't end
		// up with
		// something like "e21ff" instead of "e201ff"

		char[] charArray = inStr.toCharArray();
		byte[] byteArray = new byte[charArray.length];

		for (int i = 0; i < charArray.length; i++)
			byteArray[i] = (byte) charArray[i];

		byte[] md5Bytes = this.md5.digest(byteArray);

		StringBuffer hexValue = new StringBuffer();

		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16)
				hexValue.append("0");
			hexValue.append(Integer.toHexString(val));
		}

		return hexValue.toString().toUpperCase();
	}

	
//	public static void main(String[] args){
//		System.out.println(MD5Encode.getMD5().encode("13800138000"));
//		if(args.length != 1)
//		{
//			System.out.println("参数错误！");
//			System.exit(1);
//		}
//	}

}
