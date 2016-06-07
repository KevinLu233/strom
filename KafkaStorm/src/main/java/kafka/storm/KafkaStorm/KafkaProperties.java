package kafka.storm.KafkaStorm;
/**
 * 
 * @author lu.zhipeng
 * 初始化kafka的连接参数，包括zookeeper的连接地址及端口，kafka的连接地址及端口、topic
 * 该类主要是提供给KafkaConsumer 进行连接kafka集群的初始化连接参数
 */
public class KafkaProperties {
	final static String zkConnect = "10.50.131.80:31315,10.50.131.85:31315,10.50.131.88:31315";
    final static String groupId = "kafka-storm";
    //final static String topic = "b2b_aaa";
    final static String topic = "storm-ps";
    final static String kafkaServerURL = "10.50.131.80,10.50.131.85,10.50.131.88";
    final static int kafkaServerPort = 9092;
    final static int kafkaProducerBufferSize = 64 * 1024;
    final static int connectionTimeOut = 2000;
    final static int reconnectInterval = 1000;
    final static String clientId = "GetKafkaDataToStorm";
}
