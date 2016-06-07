package kafka.storm.KafkaStorm;
import java.util.Map;
import java.util.Properties;
import javax.validation.constraints.Null;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.Client;
import com.sun.org.apache.xpath.internal.operations.And;
import org.apache.storm.*;
import org.apache.storm.topology.TopologyBuilder;

/*
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.config__init;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;
import kafka.consumer.ConsumerConfig;
*/
import kafka.javaapi.consumer.ZookeeperConsumerConnector;
/**
 * to create Topology to get kafka'data(topic is b2b_aaa),then use storm format  
 * last, write to elasticsearch cluster 
 * @author lu.zhipeng
 *
 */
public class MyKafkaStormTopo {
	public static void main(String[] args) throws Exception {
	
		
			TopologyBuilder builder=new TopologyBuilder();
			builder.setSpout("kafka_input", new MyKafkaInput(),2);
			builder.setBolt("storm_bolt", new MyKafkaOutput(),2).shuffleGrouping("kafka_input");
			builder.setBolt("es_bolt", new MyKafkaES(),2).shuffleGrouping("storm_bolt");
			Config conf=new Config();
			conf.setDebug(false);
			
			conf.setNumWorkers(2);
			//设置acker的数量为非0,保证消息的可靠性
			conf.setNumAckers(6);
			
			if (args!=null && args.length > 0) {
				
		
			if (args!=null && args[0].equals("local")) {
				LocalCluster cluster=new LocalCluster();
				System.out.println("Start to submit Topology MyKafkaStormES_local_PS............");
				cluster.submitTopology("MyKafkaStormES_local_PS_local", conf,builder.createTopology());
				
			}else if (args!=null && args[0].equals("cluster")) {
				System.out.println("Start to submit Topology MyKafkaStormES_PS............");
				StormSubmitter.submitTopology("MyKafkaStormES_PS", conf,builder.createTopology());
			}else {
				System.out.println("请输入需要提交 Topology 的模式：local or cluster");
				System.exit(0);
			}
			}else{
				System.out.println("请输入需要提交 Topology 的模式：local or cluster");
				System.exit(0);
			}
						
			//Thread.sleep(3000);
			//cluster.shutdown();
		}			
	}


