package iie.SparkStreaming;

import org.json.JSONArray;
import org.json.JSONObject;

public class ConvertedStringToJson {
	
//	public static void main(String[] args) throws Exception {
//		creatJSON("tt", "name:String,type:String");
//	}
	
	public static String creatJSON(String topicName, String paramter) throws Exception {
		JSONObject jsonObj = new JSONObject();// 创建json格式的数据
		jsonObj.put("namespace", "udpsss");
		jsonObj.put("type", "record");
		jsonObj.put("name", topicName);
		JSONArray fieldsJsonArr = new JSONArray();// json格式的数组
		String[] fields = paramter.split(",");
		for (int i = 0; i < fields.length; i++) {
			JSONObject fieldJsonObj = new JSONObject();
			String[] nameAndType = fields[i].split(":");
			String fieldName = nameAndType[0];
			String fieldType = nameAndType[1];
			fieldJsonObj.put("name", fieldName);
			fieldJsonObj.put("type", fieldType);
			fieldsJsonArr.put(fieldJsonObj);
		}
		jsonObj.put("fields", fieldsJsonArr);
		System.out.println(jsonObj.toString());
		return jsonObj.toString();
	}

}
