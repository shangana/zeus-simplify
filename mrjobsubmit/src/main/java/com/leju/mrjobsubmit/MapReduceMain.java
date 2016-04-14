package com.leju.mrjobsubmit;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;

public class MapReduceMain {

	public static void main(String[] args) throws Exception {
		if (args != null && args.length !=2) {
			System.out.println("格式错误");
		}
		if (args != null && args.length >= 1) {
			System.out.println("jar path is :"+args[0]+".main class is :"+args[1]);
		}
		Configuration conf = new Configuration(true);
		conf.set("mapreduce.job.jar", args[0]);
		//获取当前路径，将mapreduce.job.jar写入到core-site.xml中 
		File target = new File(System.getProperty("user.dir") + File.separator + "core-site.xml");
		if (!target.exists()) {
			target.createNewFile();
		}
		conf.writeXml(new FileOutputStream(target));
		System.out.println(" mainclass invoke start!");
		Class<?> main = Class.forName(args[1]);
		Method method = main.getDeclaredMethod("main", new Class[] { String[].class });
		String[] nargs = new String[args.length - 1];
		for (int i = 1; i < args.length; i++) {
			nargs[i - 1] = args[i];
		}
		method.invoke(null, new Object[] { nargs });
	}

}
