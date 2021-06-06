package com.seyfullahbecerikli.Sensor;

import java.util.Properties;

import com.seyfullahbecerikli.Common.Position;
import com.seyfullahbecerikli.Common.Sensor;
import com.seyfullahbecerikli.Common.Target;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.JsonObject;


public class SensorOperations
{
    public static void main(String[] args) throws InterruptedException {

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
        properties.put("kafka.topic"      , "sensor");

        RunMainLoop(args, properties);
    }

    public static void RunMainLoop(String[] args, Properties properties) throws InterruptedException {

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {

            while(true) {
                Thread.sleep(1000);
                String id = "sensor-" + args[0];
                producer.send(new ProducerRecord<>(properties.getProperty("kafka.topic"), id,
                        CreateMsg(Integer.parseInt(args[1]), Integer.parseInt(args[2]))));
            }

        } finally {
            producer.close();
        }

    }

    public static String CreateMsg(int xCoord, int yCoord) {

        var gson = new Gson();

        var s = new Sensor(new Position(xCoord, yCoord));

        s.setTarget(new Target(new Position(-1, 5)));

        s.setTargetAngle(CalculateAngle(s.getPosition(), s.getTarget().getPosition()));

        s.setTargetDistance(CalculateDistance(s.getPosition(), s.getTarget().getPosition()));

        JsonObject obj = new JsonObject();

        obj.add("position",  gson.toJsonTree(s.getPosition()));
        JsonObject targetObj = new JsonObject();
        targetObj.add("position", gson.toJsonTree(s.getTarget().getPosition()));
        obj.add("target",  targetObj);
        obj.addProperty("targetAngle",  gson.toJson(s.getTargetAngle()));
        obj.addProperty("targetDistance", gson.toJson(s.getTargetDistance()));

        return gson.toJson(obj);

    }

    public static float CalculateAngle(Position sensor, Position target) {

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