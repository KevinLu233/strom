package kafka.storm.KafkaStorm;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	MyKafkaInput aInput=new MyKafkaInput(); 
    	aInput.nextTuple();
    }
}
