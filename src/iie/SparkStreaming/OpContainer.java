package iie.SparkStreaming;

import iie.udps.api.streaming.DStreamWithSchema;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class OpContainer {
	public static void main(String[] args) throws Exception {
		JavaStreamingContext jssc = new JavaStreamingContext(
				new SparkConf().setAppName("SparkStreamingOperatorTest"),
				new Duration(10000));
		
//		URL url1 = new URL("file:/usr/local/ops/ST.A.StreamingDemo/ST.A.LoadFromKafka.jar");  
//        ClassLoader classLoader1 = new URLClassLoader(new URL[] { url1 }, Thread.currentThread().getContextClassLoader());  
//		Class<?> loadFromKafka = classLoader1.loadClass("iie.SparkStreaming.LoadFromKafka");
		Class<?> loadFromKafka = Class.forName("iie.SparkStreaming.LoadFromKafka");
		Method execute1 = loadFromKafka.getDeclaredMethod("execute", new Class[]{JavaStreamingContext.class, String.class});
		Map<String,DStreamWithSchema> out1 =  (Map<String, DStreamWithSchema>) execute1.invoke(loadFromKafka.newInstance(),new Object[]{jssc, "arguments"});
		
		Class<?> UpToLow = Class.forName("iie.SparkStreaming.UpToLow");
		Method execute2 = UpToLow.getDeclaredMethod("execute", new Class[]{JavaStreamingContext.class, String.class, Map.class});
		Map<String,DStreamWithSchema> out2 =  (Map<String, DStreamWithSchema>) execute2.invoke(UpToLow.newInstance(), new Object[]{jssc, "arguments", out1});
		
		Class<?> StoreToTable = Class.forName("iie.SparkStreaming.StoreToTable");
		Method execute3 = StoreToTable.getDeclaredMethod("execute", new Class[]{JavaStreamingContext.class, String.class, Map.class});
		execute3.invoke(StoreToTable.newInstance(), new Object[]{jssc, "arguments", out2});
		
		jssc.start();
		jssc.awaitTermination();
	}
}
