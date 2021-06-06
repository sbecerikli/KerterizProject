package com.seyfullahbecerikli.Center;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.gson.Gson;

import com.seyfullahbecerikli.Common.Position;
import com.seyfullahbecerikli.Common.Sensor;
import com.seyfullahbecerikli.Common.Target;

public class CenterOperations
{
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("kafka.topic"      , "sensor");
        properties.put("compression.type" , "gzip");
        properties.put("key.deserializer"   , "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("max.partition.fetch.bytes", "2097152");
        properties.put("max.poll.records"          , "500");
        properties.put("group.id"          , "my-group");

        RunMainLoop(args, properties);
    }

    static void RunMainLoop(String[] args, Properties properties) {

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            consumer.subscribe(Collections.singletonList(properties.getProperty("kafka.topic")));

            System.out.println("Subscribed to topic " + properties.getProperty("kafka.topic"));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    Sensor val = DecodeMsg(record.value());
                    Position pos = CalculateTargetPosition(val.getPosition(), val.getTargetAngle(), val.getTargetDistance());
                    System.out.printf("%s => ", record.key());
                    System.out.printf("position(x, y): (%.1f, %.1f) \t angle: %.2f \t\t target position(x,y): (%.1f, %.1f)\n",
                            val.getPosition().getXCoordinate(), val.getPosition().getYCoordinate(),
                            val.getTargetAngle(), pos.getXCoordinate(), pos.getYCoordinate());
                }

            }
        }
    }

    public static Sensor DecodeMsg(String json) {

        Gson gson = new Gson();

        var msg = gson.fromJson(json, Sensor.class);

        var x = msg.getPosition().getXCoordinate();
        var y = msg.getPosition().getYCoordinate();
        var targetX = msg.getTarget().getPosition().getXCoordinate();
        var targetY = msg.getTarget().getPosition().getYCoordinate();

        var distance = msg.getTargetDistance();
        var angle = msg.getTargetAngle();

        msg.setPosition(new Position(x, y));
        msg.setTarget(new Target(new Position(targetX, targetY)));
        msg.setTargetAngle(angle);
        msg.setTargetDistance(distance);

        return msg;
    }

    public static Position CalculateTargetPosition(Position sensor, float angle, float distance){

        if (angle > 180)
            angle -= 180;

        var x = (float) (distance * Math.cos(Math.toRadians(angle)) + sensor.getXCoordinate());
        var y = (float) (distance * Math.sin(Math.toRadians(angle)) + sensor.getYCoordinate());

        return new Position(x, y);
    }
}