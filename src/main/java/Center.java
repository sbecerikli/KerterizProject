import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.gson.Gson;

public class Center
{
    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("kafka.topic"      , "sensor");
        properties.put("compression.type" , "gzip");
        properties.put("key.deserializer"   , "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("max.partition.fetch.bytes", "2097152");
        properties.put("max.poll.records"          , "500");
        properties.put("group.id"          , "my-group");

        runMainLoop(args, properties);
    }

    static void runMainLoop(String[] args, Properties properties) throws InterruptedException, UnsupportedEncodingException {

        // Create Kafka producer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        try {

            consumer.subscribe(Arrays.asList(properties.getProperty("kafka.topic")));

            System.out.println("Subscribed to topic " + properties.getProperty("kafka.topic"));

            while (true)
            {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records)
                {
                    Sensor val = decodeMsg(record.value());
                    Position pos = CalculateTargetPosition(val.getPosition(), val.getTargetAngle(), val.getTargetDistance());
                    System.out.printf("%s=> ", record.key());
                    System.out.printf("position(x, y): (%.1f, %.1f) \t angle: %.2f \t target position(x,y): (%.1f, %.1f)\n",
                            val.getPosition().getXCoordinate(), val.getPosition().getYCoordinate(),
                            val.getTargetAngle(), pos.getXCoordinate(), pos.getYCoordinate());
                }

            }
        }

        finally {
            consumer.close();
        }
    }

    public static Sensor decodeMsg(String json) {

        Gson gson = new Gson();

        var msg = gson.fromJson(json, Sensor.class);

        float x = msg.getPosition().getXCoordinate();
        float y = msg.getPosition().getYCoordinate();
        float targetX = msg.getTarget().getPosition().getXCoordinate();
        float targetY = msg.getTarget().getPosition().getYCoordinate();

        float distance = msg.getTargetDistance();
        float angle = msg.getTargetAngle();

        msg.setPosition(new Position(x, y));
        msg.setTarget(new Target(new Position(targetX, targetY)));
        msg.setTargetAngle(angle);
        msg.setTargetDistance(distance);

        //CalculateTargetPosition(msg.getPosition(), msg.getTargetAngle(), msg.getTargetDistance());
        return msg;
    }

    public static Position CalculateTargetPosition(Position sensor, float angle, float distance ){

        if (angle > 180)
            angle -= 180;

        float x = (float) (distance * Math.cos(Math.toRadians(angle)) + sensor.getXCoordinate());
        float y = (float) (distance * Math.sin(Math.toRadians(angle)) + sensor.getYCoordinate());

        return new Position(x, y);
    }
}