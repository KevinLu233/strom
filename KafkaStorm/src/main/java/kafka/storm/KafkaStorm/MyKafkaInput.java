package kafka.storm.KafkaStorm;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.emitter.Emitable;
import org.junit.Test;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
/**
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
**/
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * use kafka_consumer to create a kafka consumer client connector ,then use client to pull data from kafka
 * @author lu.zhipeng
 *
 */
public class MyKafkaInput extends BaseRichSpout {
		
	private static final long serialVersionUID = 6470207401785986297L;
	private SpoutOutputCollector collector;
	private Integer messageId=0;
	//public ConsumerIterator<byte[], byte[]> get_input;
	
	//public Random random_num=new Random();
	public void nextTuple() {
		// TODO Auto-generated method stub
		/**
		 * 
		 */	
		KafkaConsumer kafka_consumer=new KafkaConsumer(KafkaProperties.topic);
		ConsumerIterator<byte[], byte[]> get_input=kafka_consumer.run();
        while (get_input.hasNext()) {
            //取得kafka 输入的数据
        	
        	//System.out.println("receive：" + new String(get_input.next().message()));
            String kafka_in=new String(get_input.next().message());
            //为了保证消息的可靠性，同事设定spout 的messageId 和acker非0
            
          // messageId=random_num.nextInt(999999999);
           // collector.emit(new Values(kafka_in),messageId);
            collector.emit(new Values(kafka_in));
            
        }	
	
	}


	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("kafka_in"));
	}

	/**
	public void fail(Object msgId) {
		int in=(Integer) msgId;
		collector.emit(new Values(get_input.indexOf(in)),in);
		 }
	    **/        
	}
	
