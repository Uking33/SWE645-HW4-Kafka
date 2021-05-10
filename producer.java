import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class producer{
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
      props.put("producer.type", "async");
      props.put("bootstrap.servers", "10.43.15.133:9092");
      //props.put("metadata.broker.list", "10.43.100.131:9092");
      props.put("acks", "0");
      props.put("retries", 1);            
      props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");         
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
      
      Producer<String, String> producer = new KafkaProducer
         <String, String>(props);

      System.out.println("Start");
      for(int i = 0; i < 10; i++) {
          producer.send(new ProducerRecord<String, String>(topicName, 
             Integer.toString(i), Integer.toString(i)));  
          System.out.println("send"+i);
      }
      
      System.out.println("Message sent successfully");
      producer.close();
   }
}
