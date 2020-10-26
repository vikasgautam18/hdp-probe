package com.gautam.mantra.oozie;

import com.gautam.mantra.commons.ProbeService;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Properties;

public class ProbeOozie implements ProbeService {
    public final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
    private final Map<String, String> properties;
    private static OozieClient wc;

    public ProbeOozie(Map<String, String> properties) {
        this.properties = properties;
         wc = new OozieClient(properties.get("oozie.url"));
    }

    @Override
    public Boolean isReachable() {
        try {
            return !wc.getAvailableOozieServers().isEmpty();
        } catch (OozieClientException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean invokeWorkflow() {

        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, properties.get("oozie.app.path"));
        conf.setProperty("jobTracker", properties.get("oozie.app.jobtracker"));
        conf.setProperty("nameNode", properties.get("oozie.app.namenode"));
        conf.setProperty("queueName", properties.get("oozie.app.queue.name"));
        conf.setProperty("examplesRoot", properties.get("oozie.app.examples.root"));

        try {
            String jobId = wc.run(conf);
            System.out.println(jobId);

            while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
                System.out.println("Workflow job running ...");
                Thread.sleep(10 * 1000);
            }

            // print the final status of the workflow job
            System.out.println("Workflow job completed ...");
            System.out.println(wc.getJobInfo(jobId));
        } catch (OozieClientException | InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }
}
