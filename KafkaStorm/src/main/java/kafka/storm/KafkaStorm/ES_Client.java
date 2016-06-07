package kafka.storm.KafkaStorm;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import com.sun.javafx.collections.MappingChange.Map;
import javafx.print.JobSettings;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
//  search index 
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.omg.CORBA.PUBLIC_MEMBER;

//import org.junit.After;
//import org.junit.Before;

public class ES_Client {
	
	
	
	private static String host1="10.50.131.80";
	private static String host2="10.50.131.84";
	private static String host3="10.50.131.85";
	private static String host4="10.50.131.36";
	private static String host5="10.50.131.87";
	private static String host6="10.50.131.36";
	static java.util.Map<String, String> m=new HashMap<String,String>();
	// 设置client.transport.sniff为true来使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端
	static Settings settings=ImmutableSettings.settingsBuilder().put(m)
			.put("cluster.name", "Bestv-elasticsearch")
			.put("client.transport.ping_timeout", 100) 
			.put("client.transport.sniff", true).build();
	private static TransportClient client;
	static {
	        try {
	            Class<?> clazz = Class.forName(TransportClient.class.getName());
	            Constructor<?> constructor = clazz.getDeclaredConstructor(Settings.class);
	            constructor.setAccessible(true);
	            client = (TransportClient) constructor.newInstance(settings);
	            /**
	            client.addTransportAddress(new InetSocketTransportAddress(host1,9300));
	            client.addTransportAddress(new InetSocketTransportAddress(host2,9300));
	            client.addTransportAddress(new InetSocketTransportAddress(host3,9300));
	            client.addTransportAddress(new InetSocketTransportAddress(host4,9300));
	            client.addTransportAddress(new InetSocketTransportAddress(host5,9300));**/
	            client.addTransportAddress(new InetSocketTransportAddress(host6,9300));
	        } catch (Exception e) {
	            e.printStackTrace();
	            System.out.println("客户端连接异常！");
	            //System.exit(0);
	        } 
	    }
	  // 取得实例
    public static synchronized TransportClient getTransportClient() {
        return client;
        
    }
	
	
	
	
	
	/**
	 * 测试环境下没有问题，但是在线上环境下会出问题，会出现内存溢出和超时等问题。
	 */
	/*
	public Client ESconnector() {
		
		
		//设置ES集群的集群名称
		Settings settings=ImmutableSettings.settingsBuilder()
				.put("cluster.name","Bestv-elasticsearch").build();
	
		//建立一个transport类型的client连接
		@SuppressWarnings("resource")
		Client client=new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(host1,9300))
				.addTransportAddress(new InetSocketTransportAddress(host2,9300))
				.addTransportAddress(new InetSocketTransportAddress(host3,9300))
				.addTransportAddress(new InetSocketTransportAddress(host4,9300))
				.addTransportAddress(new InetSocketTransportAddress(host5,9300));
		System.out.println("连接成功..............");
		return client;
		
	
		/**
		XContentBuilder builder = jsonBuilder()
			    .startObject()
			        .field("user", "kimchy")
			        .field("postDate", new Date())
			        .field("message", "trying out Elasticsearch")
			    .endObject();
	   
	   String json = builder.string();
	   System.out.println("json is :"+json);
	   System.out.println("start put json to es:");
	   IndexResponse response=client.prepareIndex("twitter","tweet")
			   .setSource(json)
			   .execute()
			   .actionGet();
	   boolean created = response.isCreated();
	   System.out.println("is created or not:"+created);
	   
	}
	**/
	

}