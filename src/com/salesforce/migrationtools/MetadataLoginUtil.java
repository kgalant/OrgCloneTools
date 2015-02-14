package com.salesforce.migrationtools;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.metadata.MetadataConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class MetadataLoginUtil {

    public static MetadataConnection mdLogin(String url, String user, String pwd) throws ConnectionException {
        final LoginResult loginResult = loginToSalesforce(user, pwd, url);
        return createMetadataConnection(loginResult);
    }
    
    public static PartnerConnection soapLogin(String url, String user, String pwd) {
        
    	PartnerConnection conn = null;

        try {
           ConnectorConfig config = new ConnectorConfig();
           config.setUsername(user);
           config.setPassword(pwd);

           System.out.println("AuthEndPoint: " + url);
           config.setAuthEndpoint(url);

           conn = new PartnerConnection(config);
           
        } catch (ConnectionException ce) {
           ce.printStackTrace();
        } 

        return conn;
     }

    private static MetadataConnection createMetadataConnection(
            final LoginResult loginResult) throws ConnectionException {
        final ConnectorConfig config = new ConnectorConfig();
        config.setServiceEndpoint(loginResult.getMetadataServerUrl());
        config.setSessionId(loginResult.getSessionId());
        return new MetadataConnection(config);
    }

    private static LoginResult loginToSalesforce(
            final String username,
            final String password,
            final String loginUrl) throws ConnectionException {
        final ConnectorConfig config = new ConnectorConfig();
        config.setAuthEndpoint(loginUrl);
        config.setServiceEndpoint(loginUrl);
        config.setManualLogin(true);
        return (new PartnerConnection(config)).login(username, password);
    }
}