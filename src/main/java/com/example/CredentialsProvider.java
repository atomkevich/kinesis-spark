package com.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;

/**
 * Created by alina on 30.10.15.
 */
public abstract class CredentialsProvider {

    public static AWSCredentialsProvider getAwsSessionCredentialsProvider() {
        String path = CredentialsProvider.class.getClassLoader().getResource("key.properties").getPath();
        return new PropertiesFileCredentialsProvider(path);
    }
}
