package kafka.storm.KafkaStorm;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
/**
* @author luzhipeng
* 创建 KafkaConsumer class,从kafka读取数据到spout
* 主要实现zookeeper的连接，初始化config,创建consumer connector
* 调用该类的run()方法时返回一个ConsumerIterator<byte[], byte[]>类型的数据流到storm spout
*/
public class KafkaConsumer {
	    private final ConsumerConnector consumer;
	    private final String topic;
	    public KafkaConsumer(String topic)
	    {
	        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
	                createConsumerConfig());
	        this.topic = topic;
	    }
	    private static ConsumerConfig createConsumerConfig()
	    {
	        Properties props = new Properties();
	        props.put("zookeeper.connect", KafkaProperties.zkConnect);
	        props.put("group.id", KafkaProperties.groupId);
	        props.put("zookeeper.session.timeout.ms", "6000");
	        props.put("zookeeper.sync.time.ms", "1000");
	        props.put("auto.commit.interval.ms", "10");
	        //不设置会抛出 kafka.common.ConsumerRebalanceFailedException:错误
	        props.put("rebalance.max.retries", "5");
	        props.put("rebalance.backoff.ms", "1200");
	        return new ConsumerConfig(props);
	    }
	    
	    
	    public ConsumerIterator<byte[], byte[]> run() {
	        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put(topic, new Integer(1));
	        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
	        ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        return it;
	  
	    }
		
	}