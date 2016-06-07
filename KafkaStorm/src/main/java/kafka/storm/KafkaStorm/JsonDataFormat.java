package kafka.storm.KafkaStorm;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.commons.lang.time.DateFormatUtils;
/**
 * 
 * @author lu.zhipeng
 * create JsonDataFormat class to deal with Bolt "MyKafkaOutput" emmit MykafkaES Data
 * 1. to create a java object for using gson
 * 2. to format log_date tfor elsaticsearch
 *
 */
public class JsonDataFormat {
	private String index;
	private String type;
	private String hostname;
	private String timestamp;
	private String log_msg;
	
	
	public JsonDataFormat(String index,String type,String hostname,String timestamp,String log_msg){
		this.index=index;
		this.type=type;
		this.hostname=hostname;
		this.timestamp=timestamp;
		this.log_msg=log_msg;
		
		
	}
	public String getIndex() {
		
		return index;
		
	}
	public String getType() {
		return type;
		
	}
	public String GetHostname() {
		
		return hostname;
	}
	
	public java.util.Date GetTimestamp() {
		
		java.util.Date timestamp_format=new java.util.Date();
		//定义日期格式
		String pattern = "yyyy-MM-dd' 'HH:mm:ss,SSS";
		SimpleDateFormat dateformat=new SimpleDateFormat(pattern);
		try {
			java.util.Date timestamp_format1=dateformat.parse(timestamp);
			timestamp_format=timestamp_format1;
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			return timestamp_format;
	}

	public String GetLogMessage() {
	
	return log_msg;
	}
}
