package iie.SparkStreaming;

import java.io.IOException;
import java.util.List;

import kafka.utils.VerifiableProperties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

public class AvroSDecoder implements kafka.serializer.Decoder<String[]> {

	private DatumReader<GenericRecord> reader;

	public AvroSDecoder(VerifiableProperties props) {

		String schema="";
		try {
			schema = ConvertedStringToJson.creatJSON("sytest22", "name:string,type:string");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Schema sche = new Schema.Parser().parse(schema);
		System.out.println("sche========================" + sche.toString());
		reader = new GenericDatumReader<GenericRecord>(
				new Schema.Parser().parse(schema));
	}

	@Override
	public String[] fromBytes(byte[] arg0) {
		Decoder decoder = DecoderFactory.get().binaryDecoder(arg0, null);
		GenericRecord record = null;
		try {
			record = reader.read(null, decoder);
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		int fieldSize = record.getSchema().getFields().size();
		System.out.println("getSchemaSize===========================" + fieldSize);
		List<Field> fields = record.getSchema().getFields();
		for (int j = 0; j < fields.size(); j++) {
			System.out.println("field name===========================" + fields.get(j).name());
		}

		String[] desline = new String[fieldSize];
		for (int i = 0; i < fieldSize; i++) {
			System.out.println("record.get(" + i + ")==================" + record.get(i).toString());
			desline[i] = record.get(i).toString();
		}
		return desline;
	}
}