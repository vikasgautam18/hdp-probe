package com.gautam.mantra.commons;

import java.net.Socket;

public class Utilities {

    public boolean serverListening(String host, int port)
    {
        System.out.println("inside serverListening");
        Socket s = null;
        try
        {
            s = new Socket(host, port);
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
        finally
        {
            if(s != null)
                try {
                    s.close();
                }
                catch(Exception e){
                    e.printStackTrace();
                }
        }
    }
}
