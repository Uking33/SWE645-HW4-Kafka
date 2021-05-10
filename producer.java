import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class producer  {
    public static void main(String[] args) throws Exception{	      
    // Check arguments length value
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }
      //Assign topicName to string variable
      String topicName = args[0].toString();
      String json = args[1].toString();      
      
      //Props
      Properties props = new Properties();
      //props.put("producer.type", "async");
      props.put("bootstrap.servers", "localhost:9092");
      //props.put("metadata.broker.list", "10.43.100.131:9092");
	  //props.put("metadata.broker.list", "10.22.10.139:9092");
      props.put("acks", "0");
      props.put("retries", 1);            
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");         
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      
      //Send
      Producer<String, String> producer = new KafkaProducer
         <String, String>(props);
      Gson gson=new Gson();
      Map<String, Object> map = gson.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
      String uid = (String)map.get("uid");
      ProducerRecord<String, String> record = 
    		  new ProducerRecord<String, String>(topicName, uid, json);
      producer.send(record);
      System.out.println("Message sent successfully");
      producer.close();
   }
}
