package kafka.storm.KafkaStorm;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.simple.JSONObject;
import org.apache.storm.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import clojure.lang.IFn.OLL;

import groovy.json.JsonBuilder;
import javassist.runtime.Desc;
import kafka.utils.Json;
import scala.annotation.elidable;

public class MyKafkaOutput extends BaseRichBolt {
	
	private static final long serialVersionUID= -917956850826439496L;
	private OutputCollector collector;
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String word = (String) input.getValue(0);  
        //String out = "Storm get Kafka Data: ";
		
        /**
        System.out.println(out);
        System.out.println(word);
        System.out.println("");
        System.out.println("");
        **/
        
        //System.out.println("***************************************************************");
        //首先判断读入的信息是否是由syslog-ng传入kafka的正常的日志
        if (word.length() > 40 && "<13>".equals(word.substring(0,4))) {
        // String[] result=word.split("@aaa");	
        String[] result=word.split("@ps");
        //取得ip和索引名字
        String[] result1=result[0].split(" ");
        //判断字符串里面是否有+
        //if (result1[4].length() > 9 && (result1[4].substring(result1[4].length()-9,result1[4].length()-8)).equals("+")) {
		if((result1.length >= 4) &&
		   (result1[(result1.length-1)].substring(result1[(result1.length-1)].length()-9, result1[(result1.length-1)].length()-8)).equals("+")
		   ) {	
	
        String[] result2=result1[4].split("\\+");
        String ip=result2[0];
        
        
       
        	 String index_name=result2[1];
             //System.out.println("hostname is: "+ip);
            // System.out.println("Index_name is:"+index_name);
             
		
       
        //判断日志是否带有时间，是否是正常的日志
     
        String log=result[1];
      //判断log是否符合需要的格式
        if (log.length() > 26) {
			
     		
        String[] tmp=result[1].split(" ");
        //将处理后需要发射到下一个bolt的日志转为json格式，创建一个json对象
        JSONObject es_json=new JSONObject();
        if ((tmp[1].substring(0, 2)).equals("20")) {
			
        	//取得原始服务器中的日志数据
            String[] result3=log.split(" 201");
            String log_message=result3[1];
            //取出日志的时间
            String log_time="201"+log_message.substring(0, 20);             
            //System.out.println("timestamp is :"+log_time);
            String log_msg="201"+log_message;
            //System.out.println("normal log_message is :"+"201"+log_message);
            
            //建立json数据，传到下一个bolt进行处理
            Map<String,String> es_map=new HashMap<>();
            es_map.put("hostname", ip);
            es_map.put("index", index_name);
            es_map.put("type", "aaa-"+log_time.substring(0, 10));
            es_map.put("timestamp", log_time);
            es_map.put("log_msg", log_msg);
            
            
            //JsonBuilder es_json=new JsonBuilder(es_map.toString());
            es_json.putAll(es_map);
            //System.out.println("json data:");
           // System.out.println(es_json);
            //System.out.println(""); 
            
            collector.emit(input,new Values(es_json.toString()));              
            collector.ack(input);
            //collector.emit(new Values(es_json.toString()));
        	
		}else {
			//String log_message=tmp[1];
			//System.out.println("not normal log_message is :"+log_message);
			//collector.emit(new Values(log_message));
		   
			
        	
		}
        }
        
        }
       
        //System.out.println("***************************************************************");
        
        //collector.emit(new Values(ip));
        //collector.emit(new Values(index_name));     
        
    	}
       
	}

	public void prepare(Map stormConf, TopologyContext context , OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("es_json"));
		
	}

	
}
