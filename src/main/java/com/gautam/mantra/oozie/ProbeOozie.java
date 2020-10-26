package com.gautam.mantra.oozie;

import com.gautam.mantra.commons.ProbeService;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

public class ProbeOozie implements ProbeService {
    public final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    private final Map<String, String> properties;

    public ProbeOozie(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Boolean isReachable() {

        try {
            OozieClient wc = new OozieClient(properties.get("oozie.url"));
            return !wc.getAvailableOozieServers().isEmpty();
        } catch (OozieClientException e) {
            e.printStackTrace();
            return false;
        }
    }
}
