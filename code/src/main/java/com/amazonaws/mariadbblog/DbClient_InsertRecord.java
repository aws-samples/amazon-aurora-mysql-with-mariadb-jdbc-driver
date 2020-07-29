package com.amazonaws.mariadbblog;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.*;
import java.util.Base64;

public class DbClient_InsertRecord {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";

    // Database credentials
    static String USER = null;
    static String PASS = null;
    static String DB_URL = null;

    public static void populateSecret() {
        String secretName = "db-blog";
        String region = "us-west-2";

        // Create a Secrets Manager client
        AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region)
                .build();

        // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // We rethrow the exception by default.

        String secret = null;

        try {
            GetSecretValueResult getSecretValueResult = client.getSecretValue(new GetSecretValueRequest()
                    .withSecretId(secretName));

            // Decrypts secret using the associated KMS CMK.
            // Depending on whether the secret is a string or binary, one of these fields will be populated.
            if (getSecretValueResult.getSecretString() != null) {
                secret = getSecretValueResult.getSecretString();
            } else {
                secret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
            }

            //System.out.println(" Secret values...");
           // System.out.println(secret);

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(secret);
            USER = jsonNode.get("username").asText();
            PASS = jsonNode.get("password").asText();
            DB_URL = String.format(
                    "jdbc:mariadb:aurora//%s:%s/" + "?allowMultiQueries=true&useSSL=false",
                    jsonNode.get("host").asText(),
                    jsonNode.get("port").asText());
        } catch (DecryptionFailureException e) {
            // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InternalServiceErrorException e) {
            // An error occurred on the server side.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidParameterException e) {
            // You provided an invalid value for a parameter.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidRequestException e) {
            // You provided a parameter value that is not valid for the current state of the resource.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (ResourceNotFoundException e) {
            // We can't find the resource that you asked for.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (JsonProcessingException e) {
            // We can't deserialize the secret String.
            // Deal with the exception here, and/or rethrow at your discretion.
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String masterServer = "";
        String slaveServer = "";
        try {
            // STEP 2: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // STEP 3: Open a connection
            populateSecret();
            System.out.println("Connecting to database...");

            try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
                System.out.println("Connected successfully to database...");

                // STEP 4: Execute a query
                System.out.println("Inserting record into table in the connected database...");
                try (Statement stmt = conn.createStatement()) {
                    //String sql =   "SELECT SERVER_ID, SESSION_ID FROM INFORMATION_SCHEMA.REPLICA_HOST_STATUS";
                    String sql = "USE auroradbtest;" + "INSERT INTO VENDORS (id, name, CEO, VAT_Number) VALUES (2, 'Amazon', 'Jeff Bezos', 22222)";
                   // System.out.println("Printing master and slave nodes of the db cluster...");

                    /*
                    ResultSet rs = stmt.executeQuery("SELECT SERVER_ID, SESSION_ID FROM INFORMATION_SCHEMA.REPLICA_HOST_STATUS");
                    while (rs.next()) {
                        if (rs.getString(2).equals("MASTER_SESSION_ID"))
                            masterServer = rs.getString(1); // the node that is the master currently has a session named MASTER_SESSION_ID
                            //System.out.println(rs);
                        else
                            slaveServer += (("".equals(slaveServer)) ? "" : ",") + rs.getString(1); // other nodes ares replicas

                        //Printing out endpoint with their session_Id indicating which is master or replica
                        System.out.println(rs.getString(1)+ " "+rs.getString(2));
                    }

                    */
                    //String sql = "CREATE TABLE VENDORS " + "(id INTEGER not NULL, " + " name VARCHAR(255), "
                    //+ " CEO VARCHAR(255), " + " VAT_Number INTEGER, " + " PRIMARY KEY ( id ))";

                    stmt.executeUpdate(sql);
                    System.out.println("Record inserted successfully...");
                    // stmt.executeUpdate(sql);
                    // System.out.println("Deleted table in a given database...");
                } catch (SQLException e) {
                    // Handle errors for JDBC
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                // Handle errors for JDBC
                e.printStackTrace();
            }
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        }

        System.out.println("Goodbye!");
    }// end main
}// end JDBCExample