import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sun.javafx.collections.MappingChange.Map;

import net.sf.json.JSONObject;

public class MyJsonTest {

	public static void main(String[] args) {
		// TODO Auto-generated method 
		// ����һ��json�ַ���
		org.json.simple.JSONObject json=new org.json.simple.JSONObject();
		java.util.Map<String, String> map=new HashMap<>();
		map.put("name", "luzhipeng");
		map.put("work", "IT");
		map.put("company", "Bestv");
		json.putAll(map);
		System.out.println("now json string is: "+json.toString());
		Gson gson=new Gson();
		GetData getdata=gson.fromJson(json.toString(), GetData.class);
		
		System.out.println("name is "+getdata.getName());
		System.out.println("work is " +getdata.getWork());
		System.out.println("company is "+getdata.geCompany());
		
		
	}

}
