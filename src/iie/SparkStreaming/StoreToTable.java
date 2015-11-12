package iie.SparkStreaming;

import iie.udps.api.streaming.DStreamWithSchema;
import iie.udps.common.hcatalog.SerHCatOutputFormat;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.thrift.TException;

import scala.Tuple2;

public class StoreToTable implements Serializable {
	private static final long serialVersionUID = 6612375645495078109L;
	public void execute(JavaStreamingContext jssc, String arguments,
			Map<String,DStreamWithSchema> record) {
		Configuration conf = new Configuration();
		String serialName = String.valueOf(System.currentTimeMillis());
		String dbName = "test";
		String tbName = "sy" + serialName;
		HCatSchema schema0 = record.get("outport1").getSchema();
		System.out.println("oooooooooooooooooooooooooooooo");
		System.out.println("schema:" + schema0);
		createTable(dbName, tbName, schema0);
		Job outputJob = null;
		try {
			outputJob = Job.getInstance(conf);
			outputJob.setJobName("SparkStreamingTest");
			outputJob.setOutputKeyClass(WritableComparable.class);
			outputJob.setOutputValueClass(SerializableWritable.class);

			SerHCatOutputFormat.setOutput(outputJob,
					OutputJobInfo.create(dbName, tbName, null));
			HCatSchema schema = SerHCatOutputFormat.getTableSchema(outputJob
					.getConfiguration());
			System.out.println("schema2222222222222222222222222222222" + schema.toString());
			SerHCatOutputFormat.setSchema(outputJob, schema);
			outputJob.setOutputFormatClass(SerHCatOutputFormat.class);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		record.get("outport1").getDStream().print();
	
		record.get("outport1").getDStream().mapToPair(
						new PairFunction<SerializableWritable<HCatRecord>, NullWritable, SerializableWritable<HCatRecord>>() {
							private static final long serialVersionUID = 1741555917449626517L;
							public Tuple2<NullWritable, SerializableWritable<HCatRecord>> call(SerializableWritable<HCatRecord> record){
								System.out.println("record>>>>>>>>>>>>>>>>>>>>>>>>toString");
								System.out.println(record.toString());
								return new Tuple2<NullWritable, SerializableWritable<HCatRecord>>(
										NullWritable.get(), record);
							}
						})
				.saveAsNewAPIHadoopFiles("", "", WritableComparable.class,
						SerializableWritable.class, SerHCatOutputFormat.class,
						outputJob.getConfiguration());
	}
	
	/**
	 * 通过hcatalog的schema在hive中的建表方法，使用RC存储，表已存在则先删除。
	 * 
	 * @param dbName
	 *            数据库名
	 * @param tblName
	 *            表名
	 * @param schema
	 *            表结构
	 */
	public static void createTable(String dbName, String tblName,
			HCatSchema schema) {
		HiveMetaStoreClient client = null;
		try {
			HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
			try {
				client = HCatUtil.getHiveClient(hiveConf);
			} catch (MetaException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			if (client.tableExists(dbName, tblName)) {
				client.dropTable(dbName, tblName);
			}
		} catch (TException e) {
			e.printStackTrace();
		}

		List<FieldSchema> fields = HCatUtil.getFieldSchemaList(schema
				.getFields());
		System.out.println(fields);
		Table table = new Table();
		table.setDbName(dbName);
		table.setTableName(tblName);

		StorageDescriptor sd = new StorageDescriptor();
		sd.setCols(fields);
		table.setSd(sd);
		sd.setInputFormat(RCFileInputFormat.class.getName());
		sd.setOutputFormat(RCFileOutputFormat.class.getName());
		sd.setParameters(new HashMap<String, String>());
		sd.setSerdeInfo(new SerDeInfo());
		sd.getSerdeInfo().setName(table.getTableName());
		sd.getSerdeInfo().setParameters(new HashMap<String, String>());
		sd.getSerdeInfo().getParameters()
				.put(serdeConstants.SERIALIZATION_FORMAT, "1");
		sd.getSerdeInfo().setSerializationLib(
				org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class
						.getName());
		Map<String, String> tableParams = new HashMap<String, String>();
		table.setParameters(tableParams);
		try {
			client.createTable(table);
			System.out.println("Create table successfully!");
		} catch (TException e) {
			e.printStackTrace();
			return;
		} finally {
			client.close();
		}
	}
}
