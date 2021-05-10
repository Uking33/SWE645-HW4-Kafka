import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;

public class producer extends Thread{
    private final Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();
	public producer(String topic)
    {

		//props.put("producer.type", "async");
		//props.put("bootstrap.servers", "localhost:9092");
		//props.put("metadata.broker.list", "10.43.100.131:9092");
        props.put("metadata.broker.list", "10.22.10.139:9092");
		props.put("acks", "0");
		props.put("retries", 1);            
		//props.put("key.serializer", 
		//"org.apache.kafka.common.serialization.StringSerializer");         
		//props.put("value.serializer", 
		//"org.apache.kafka.common.serialization.StringSerializer");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        producer = new Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }
    public static void main(String[] args) throws Exception{	      
    // Check arguments length value
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }
      
      //Assign topicName to string variable
      String topicName = args[0].toString();
      
      // create instance for properties to access producer configs   
      Properties props = new Properties();
      Producer<String, String> producer = new KafkaProducer
         <String, String>(props);
      
      StudentBean stu = new StudentBean();
	  String json = new Gson().toJson(stu);
      
      ProducerRecord<String, String> record = 
    		  new ProducerRecord<String, String>(topicName, stu.getUid(), json);
      producer.send(record);
      
      System.out.println("Message sent successfully");
      producer.close();
   }
}
