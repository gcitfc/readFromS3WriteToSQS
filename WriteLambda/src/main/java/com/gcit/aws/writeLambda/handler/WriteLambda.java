package com.gcit.aws.writeLambda.handler;

import java.io.ByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.*;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class WriteLambda implements RequestHandler<S3Event, String> {

	@Override
	public String handleRequest(S3Event s3event, Context context) {
	    LambdaLogger logger = context.getLogger();
	    AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
	    final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

		// Parse S3 Event
		S3EventNotificationRecord record = s3event.getRecords().get(0);
		String srcBucket = record.getS3().getBucket().getName();
		String srcKey = record.getS3().getObject().getUrlDecodedKey();
		
		logger.log("Received new file " + srcBucket + ":" + srcKey);
		
		// Read the file
		//AmazonS3Client s3Client = new AmazonS3Client();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                srcBucket, srcKey));
        InputStream objectData = s3Object.getObjectContent();
        
        String content = "     ";
		try {
			content = readString(objectData);
		} catch (IOException e) {
			e.printStackTrace();
		}
        logger.log("Received content with first 5 character: " + content.substring(0, 5));
               
        List<String> list = spliteString(content);
        
        for(int i = 0; i < list.size(); i++) {
        	SendMessageRequest send_msg_request = new SendMessageRequest("https://sqs.us-east-1.amazonaws.com/972085099296/readnwrite.fifo", list.get(i));
            send_msg_request.setMessageGroupId("groupone");
            sqs.sendMessage(send_msg_request);
        }
        logger.log("Content sent to SQS");
        
        return "OK";
       
	}
	
	public static String readString(InputStream inputStream) throws IOException {

	    ByteArrayOutputStream into = new ByteArrayOutputStream();
	    byte[] buf = new byte[4096];
	    for (int n; 0 < (n = inputStream.read(buf));) {
	        into.write(buf, 0, n);
	    }
	    into.close();
	    return new String(into.toByteArray(), "UTF-8");
	}
	
	public static List<String> spliteString(String st) {
		int size = 256;
		List<String> ret = new ArrayList<String>((st.length() + size - 1) / size);
	    for (int start = 0; start < st.length(); start += size) {
	        ret.add(st.substring(start, Math.min(st.length(), start + size)));
	    }
	    return ret;
	}

}
