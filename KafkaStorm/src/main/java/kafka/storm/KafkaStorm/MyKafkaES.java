package kafka.storm.KafkaStorm;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.simple.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.storm.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import groovy.json.JsonBuilder;
import kafka.utils.Json;
import net.sf.json.*;

public class MyKafkaES extends BaseRichBolt{
	
	
	private static final long serialVersionUID= -917956850826439496L;
	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
	
		String msg = (String) input.getValue(0);				
		/**System.out.println("MykafkaES get data is:");
		System.out.println(msg);
		System.out.println("");**/
		
		/**
		 * 解析 json 字符串
		 * 
		 */
		//创建Gson对象，将json 字符串转化为java JsonDataFormat对象
		//使用JsonDataFormat对象里面定义的方法获取 json 字符串中的数据
				Gson gson=new Gson();
				JsonDataFormat json_msg=gson.fromJson(msg, JsonDataFormat.class);			
		//建立elasticsearch连接
		//由于发射过快，10ms秒发射一条数据,每条数据数据写完后需要关闭连接，等待30ms,40ms入库一条数据
		
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ES_Client es_client=new ES_Client();
		Client es=es_client.getTransportClient();
	   try {
		
        //建立日志体的json
		XContentBuilder log_builder = jsonBuilder()
			    .startObject()
			        .field("@timestamp", json_msg.GetTimestamp())
			        .field("log_message", json_msg.GetLogMessage())
			        .field("hostname",json_msg.GetHostname())
			    .endObject();
	   
	   String log_json = log_builder.string();
		
	   
	   /**
	    * 一条一条写入ES
	    */
	   /**
	   System.out.println("需要写入elasticsearch 集群的log_json 是 :\n"+log_json.toString());
	   System.out.println("start put json to es:");
	   IndexResponse response=es.prepareIndex("storm-"+json_msg.getIndex(),"kafka-storm"+json_msg.getType())
			   .setSource(log_json)
			   .execute()
			   .actionGet();
	   boolean created = response.isCreated();
	   System.out.println("是否写入成功:"+created);
	   **/
	   	   
	   
	   //使用bulk 进行多条写入es集群
	   System.out.println("需要写入的数据是： "+log_json);
	   BulkProcessor bulkProcessor=BulkProcessor.builder(es, 
				  new Listener() {
			        @Override
					public void beforeBulk(long executionId, BulkRequest request) {
						// TODO Auto-generated method stub
						
					}
			        @Override
					public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
						// TODO Auto-generated method stub
						
					}
			        @Override
					public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
						// TODO Auto-generated method stub
						
					}
				}).setBulkActions(1000)//10000条数据写入一次
				  .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.KB)) 
				  .setFlushInterval(TimeValue.timeValueSeconds(5))// 5秒写入一次
			      .setConcurrentRequests(200) //允许多少个请求去执行
			      .build();
	   			
	   			System.out.println("ES_Bolt:    开始批量写入elastic 集群.........");
		  		bulkProcessor.add(new IndexRequest("storm-"+json_msg.getIndex(),"kafka-storm-"+json_msg.getType()).source(log_json));
		  		System.out.println("ES_Bolt:    完成批量写入elastic 集群");
		  		System.out.println();
		  		bulkProcessor.awaitClose(20, TimeUnit.SECONDS);
	   	   	  	//es.close();
	   
	   } catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	//  collector.emit(new Values(msg));
	    collector.emit(input,new Values(msg));
	    collector.ack(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}



}
