package cn.com.yyft.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
	
	/**
	 * 根据key获取properties文件的value
	 * @param key
	 * @return
	 */
	public static String getRelativePathValue(String key){
		Properties properties = new Properties();
		InputStream in = PropertiesUtils.class.getResourceAsStream("/resources/conf.properties");
		try {
			properties.load(in);
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (String)properties.get(key);
	}
	public static void main(String[] args) {
		Properties properties = new Properties();
		System.out.println(getRelativePathValue("brokers"));
	}
}
