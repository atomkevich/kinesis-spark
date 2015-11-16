package com.example;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Created by alina on 16.11.15.
 */
public class KinesisWordProducerASL {

    public static void main(String[] args) throws InterruptedException {
        String stream = "test";
        String endpoint = "kinesis.us-east-1.amazonaws.com";

        String recordsPerSecond = "10";
        String wordsPerRecord = "10";

        Map<String, Integer> totals = generate(stream, endpoint, recordsPerSecond, wordsPerRecord);

        // Print the array of (word, total) tuples
        System.out.println("Totals for the words sent");
        for (Map.Entry<String, Integer> entry : totals.entrySet()) {
            System.out.println(entry.getKey() + " ------ " + entry.getValue());
        }
    }


    public static Map<String, Integer> generate(String stream, String endpoint, String recordsPerSecond, String wordsPerRecord) throws InterruptedException {

        List<String> randomWords = Lists.newArrayList("spark", "you", "are", "my", "father");
        HashMap<String, Integer> total = Maps.newHashMap();


        // Create the low-level Kinesis Client from the AWS Java SDK.
        AmazonKinesisClient kinesisClient =
                new AmazonKinesisClient(CredentialsProvider.getAwsSessionCredentialsProvider());

        kinesisClient.setEndpoint(endpoint);

        System.out.println("Putting records onto stream:  " + stream);

        // Iterate and put records onto the stream per the given recordPerSec and wordsPerRecord
        for (int i = 1; i < 10; i++) {
            // Generate recordsPerSec records to put onto the stream
            for (int recordNum = 1; recordNum < Integer.valueOf(recordsPerSecond); recordNum++) {
                StringBuilder data = new StringBuilder();
                for (int x = 1; x < Integer.valueOf(wordsPerRecord); x++) {
                    int randomWordIdx = new Random().nextInt(randomWords.size());
                    String randomWord = randomWords.get(randomWordIdx);
                    Integer wordCount = total.get(randomWord);
                    total.put(randomWord, (wordCount == null) ? 1 : wordCount + 1);
                    data.append(randomWord + " ");
                }

                String partitionKey = "partitionKey" + recordNum;
                PutRecordRequest putRecordRequest = new PutRecordRequest().withStreamName(stream)
                        .withPartitionKey(partitionKey)
                        .withData(ByteBuffer.wrap(data.toString().getBytes()));
                PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
            }


            // Sleep for a second
            Thread.sleep(1000);
            System.out.println("Sent " + recordsPerSecond + " records");
        }

        // Convert the totals to (index, total) tuple
        return total;
    }

}
