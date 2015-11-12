package iie.SparkStreaming;

import iie.udps.api.streaming.DStreamWithSchema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class UpToLow implements Serializable {
	private static final long serialVersionUID = 3655484892882276916L;
	public Map<String,DStreamWithSchema> execute(JavaStreamingContext ssc, String arguments, Map<String,DStreamWithSchema> record){
		DStreamWithSchema dstreamSchema = record.get("outport1");
		final HCatSchema schema = dstreamSchema.getSchema();
		JavaDStream<SerializableWritable<HCatRecord>> dstream = dstreamSchema.getDStream().map(new Function<SerializableWritable<HCatRecord>, SerializableWritable<HCatRecord>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public SerializableWritable<HCatRecord> call(
					SerializableWritable<HCatRecord> record) throws Exception {
				String lower = record.value().get(0).toString().toLowerCase();
				record.value().set(0, lower);;
				return record;
			}
			
		});
		DStreamWithSchema ds = new DStreamWithSchema("outport1", schema, dstream);
		Map<String,DStreamWithSchema> out = new HashMap<String, DStreamWithSchema>();
		out.put("outport1", ds);
		return out;
	}
}
