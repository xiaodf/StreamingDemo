package iie.SparkStreaming;

import iie.udps.api.streaming.DStreamWithSchema;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class LoadFromKafka implements Serializable {

	private static final long serialVersionUID = 3655484892882276916L;

	
	
	public Map<String,DStreamWithSchema> execute(JavaStreamingContext ssc,String arguments) {		
		String zkQuorum = "172.16.8.103:2181/kafka";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
		String timestamp = sdf.format(new Date());
		String groupId  = "sy" + timestamp;
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("zookeeper.connect", zkQuorum);
		kafkaParams.put("group.id", groupId);
		kafkaParams.put("auto.offset.reset", "smallest");// topic数据可以重复读取
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put("hive-test-sytest1", 8);
		JavaPairReceiverInputDStream<String, String[]> messages = KafkaUtils.createStream(ssc, String.class, String[].class,
				StringDecoder.class, AvroSDecoder.class, kafkaParams,
				topicMap, StorageLevel.MEMORY_AND_DISK_SER_2());
		JavaDStream<Tuple2<String, String[]>> dstream = messages.toJavaDStream();
		JavaDStream<SerializableWritable<HCatRecord>> dstreamRecord = dstream.map(new Function<Tuple2<String, String[]>, SerializableWritable<HCatRecord>>(){
			private static final long serialVersionUID = 1232312314354354L;
			@Override
			public SerializableWritable<HCatRecord> call(Tuple2<String, String[]> line) throws Exception {
				String[] fields = line._2;
				DefaultHCatRecord record = new DefaultHCatRecord(
						fields.length);
				record.set(0, fields[0]);
				record.set(1, fields[1]);
				return new SerializableWritable<HCatRecord>(record);
			}
			
		});
		String schema = "name:String,type:String";
		DStreamWithSchema ds = new DStreamWithSchema("outport1", getHCatSchema(schema), dstreamRecord);
		Map<String,DStreamWithSchema> out = new HashMap<String, DStreamWithSchema>();
		out.put("outport1", ds);
		return out;
	}
	
	public static HCatSchema getHCatSchema(String schema) {
		String[] fields = schema.split(",");
		String[] fieldNames = new String[fields.length];
		String[] fieldTypes = new String[fields.length];
		for (int i = 0; i < fields.length; ++i) {
			String[] nameAndType = fields[i].split(":");
			fieldNames[i] = nameAndType[0];
			fieldTypes[i] = nameAndType[1];
		}
		List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(fields.length);

		for (int i = 0; i < fields.length; i++) {
			HCatFieldSchema.Type type = HCatFieldSchema.Type.valueOf(fieldTypes[i].toUpperCase());
			try {
				fieldSchemas.add(new HCatFieldSchema(fieldNames[i], type, ""));
			} catch (HCatException e) {
				e.printStackTrace();
			}
		}
		return new HCatSchema(fieldSchemas);
		
	}
}
