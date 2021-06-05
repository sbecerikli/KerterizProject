import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Properties;
import java.util.Random;

import kafka.utils.Json;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.JsonObject;


public class SensorOperations
{
    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks"             , "0");
        properties.put("retries"          , "1");
        properties.put("batch.size"       , "20971520");
        properties.put("linger.ms"        , "33");
        properties.put("max.request.size" , "2097152");
        properties.put("compression.type" , "gzip");
        properties.put("key.serializer"   , "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("kafka.topic"      , "sampleTopic");

        runMainLoop(args, properties);
    }

    public static void runMainLoop(String[] args, Properties properties) throws InterruptedException, UnsupportedEncodingException {

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {

            while(true) {

                Thread.sleep(1000);
                String id = "sensor-" + args[0];
                producer.send(new ProducerRecord<String, String>(properties.getProperty("kafka.topic"), id,
                        CreateMsg(id, Integer.parseInt(args[1]), Integer.parseInt(args[2]))));

            }

        } finally {

            producer.close();
        }

    }

    public static String CreateMsg(String id, int xcoord, int ycoord) throws UnsupportedEncodingException {

        Gson gson = new Gson();

        String timestamp = new Timestamp(System.currentTimeMillis()).toString();

        Position sensorPosition = new Position(xcoord, ycoord); //new Position(CreateRandomPosition(-1000, 1000), CreateRandomPosition(-1000, 1000));

        Sensor s = new Sensor(sensorPosition);

        Position targetPosition = new Position(-1, 5); //new Position(CreateRandomPosition(-1000, 1000), CreateRandomPosition(-1000, 1000));

        Target target = new Target(targetPosition);

        s.setTargetAngle(CalculateAngle(sensorPosition, targetPosition));

        s.setTargetDistance(CalculateDistance(sensorPosition, targetPosition));

        JsonObject obj = new JsonObject();

        /*obj.addProperty("id", id);
        obj.addProperty("timestamp", timestamp);*/

        obj.add("position",  gson.toJsonTree(s.getPosition()));
        JsonObject targObj = new JsonObject();
        targObj.add("position", gson.toJsonTree(target.getPosition()));
        obj.add("target",  targObj);
        obj.addProperty("targetAngle",  gson.toJson(s.getTargetAngle()));
        obj.addProperty("targetDistance", gson.toJson(s.getTargetDistance()));

        //obj.addProperty("data", Base64.getEncoder().encodeToString("this is my message data ...".getBytes("utf-8")));
        String json = gson.toJson(obj);

        return json;

    }

    private static int CreateRandomPosition(int min, int max) {
        Random r = new Random();
        return r.ints(min, (max + 1)).findFirst().getAsInt();
    }

    public static float CalculateAngle(Position sensor, Position  target) {

        double angle = Math.atan2(target.getXCoordinate() - sensor.getXCoordinate(),
                                    target.getYCoordinate() - sensor.getYCoordinate()) * 180.0 / Math.PI;

        if(angle < 0)
            angle += 360;

        return (float) angle;
    }

    public static float CalculateDistance(Position sensor, Position target){
        return (float) Math.sqrt(Math.pow(target.getXCoordinate() - sensor.getXCoordinate(), 2) +
                            Math.pow(target.getYCoordinate() - sensor.getYCoordinate(), 2));
    }
}