package com.gautam.mantra.zeppelin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProbeZeppelin {

    private Map<String, String> properties;
    private static final CookieManager cookieManager = new CookieManager();
    public static final Logger logger =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

    public ProbeZeppelin(Map<String, String> properties) {
        this.properties = properties;
    }

    public boolean storeSessionCookie(){
        boolean returnValue = false;
        Configuration conf= new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, properties.get("zeppelin.jceks"));

        try {
            final CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);
            final CredentialProvider.CredentialEntry entry = provider
                    .getCredentialEntry(properties.get("zeppelin.password.alias"));

            URL url = new URL(properties.get("zeppelin.login.url"));
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            connection.setRequestMethod("POST");
            connection.setReadTimeout(10000);
            connection.setConnectTimeout(15000);
            connection.setDoOutput(true);

            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("userName", properties.get("zeppelin.login.username")));
            params.add(new BasicNameValuePair("password", String.valueOf(entry.getCredential())));

            OutputStream outputStream = connection.getOutputStream();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            writer.write(getQuery(params));
            writer.flush();
            writer.close();
            outputStream.close();

            connection.connect();
            List<String> cookiesHeader = connection.getHeaderFields().get("Set-Cookie");

            if(cookiesHeader != null){
                for (String cookie : cookiesHeader) {
                    if (HttpCookie.parse(cookie).get(0).getName().equals("JSESSIONID")) {
                        try {
                            cookieManager.getCookieStore().add(url.toURI(), HttpCookie.parse(cookie).get(0));
                            returnValue = true;
                            break;
                        } catch (URISyntaxException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            logger.info(String.format("POST to %s resulted with a response code :: %d and message :: %s",
                    connection, connection.getResponseCode(), connection.getResponseMessage()));
            logger.info("JSESSION Cookie in use:: " + cookieManager.getCookieStore().getCookies().get(0).toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    /**
     * This method invokes a zeppelin notebook
     * @param zeppelinURL
     * @param noteId
     * @return
     */
    public boolean invokeZeppelinNote(String zeppelinURL, String noteId){
        boolean result = false;
        CookieHandler.setDefault(cookieManager);
        try{
            URL url = new URL(zeppelinURL + "/" + noteId);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setReadTimeout(10000);
            connection.setConnectTimeout(15000);

            connection.setRequestProperty("Cookie", cookieManager.getCookieStore().getCookies().get(0).toString());
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");

            connection.connect();

            logger.info(String.format("POST to %s resulted with a response code :: %d and message :: %s",
                    connection, connection.getResponseCode(), connection.getResponseMessage()));
            result = connection.getResponseCode() == 200;
            connection.disconnect();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    //TODO: verify results

    private String getQuery(List<NameValuePair> params) throws UnsupportedEncodingException {
        StringBuilder query = new StringBuilder();
        boolean first = true;

        for (NameValuePair param: params) {
            if(first){
                first = false;
            } else {
                query.append("&");
            }

            query.append(URLEncoder.encode(param.getName(), StandardCharsets.UTF_8.displayName()));
            query.append("=");
            query.append(URLEncoder.encode(param.getValue(), StandardCharsets.UTF_8.displayName()));
        }
        return query.toString();
    }

    public boolean verifyResult(){

        return false;
    }
}
