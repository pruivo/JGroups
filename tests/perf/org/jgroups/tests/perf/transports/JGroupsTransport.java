package org.jgroups.tests.perf.transports;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.tests.perf.Receiver;
import org.jgroups.tests.perf.Transport;
import org.jgroups.tests.perf.Configuration;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.util.*;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 */
public class JGroupsTransport extends org.jgroups.ReceiverAdapter implements Transport  {
    Properties config=null;
    Configuration cfg=null;
    JChannel   channel=null;
    Thread     t=null;
    String     props=null;
    String     group_name="perf";
    Receiver   receiver=null;
    boolean    jmx=false;

    public JGroupsTransport() {

    }

    public Object getLocalAddress() {
        return channel != null? channel.getAddress() : null;
    }


    public String help() {
        return "[-props <props>]";
    }

    public void create(Properties properties) throws Exception {
        this.config=properties;
        props=config.getProperty("props");
        jmx=Boolean.parseBoolean(this.config.getProperty("jmx"));
        channel=new JChannel(props);
        channel.setReceiver(this);
        String name=config.getProperty("name");
        if(name != null)
            channel.setName(name);
    }

    public void create(Configuration config) throws Exception {
        this.cfg=config;
        String[] args=config.getTransportArgs();
        if(args != null) {
            for(int i=0; i < args.length; i++) {
                String tmp=args[i];
                if(tmp.equals("-props")) {
                    props=args[++i];
                    continue;
                }
                throw new IllegalArgumentException(tmp + " is not known (options: " + help() + ")");
            }
        }
        jmx=config.isJmx();
        channel=new JChannel(props);
        channel.setReceiver(this);
    }

    public void start() throws Exception {
        channel.connect(group_name);
        if(jmx) {
            MBeanServer server=Util.getMBeanServer();
            if(server == null) {
                throw new Exception("No MBeanServers found;" +
                        "\nneeds to be run with an MBeanServer present, or inside JDK 5");
            }
            JmxConfigurator.registerChannel(channel, server, "jgroups.perf", channel.getClusterName() , true);
        }
    }

    public void stop() {
        if(channel != null) {
            // we give others a chance to disconnect first
            if(isCoordinator(channel)) {
                for(int i=0; i < 10; i++) {
                    Util.sleep(300);
                    if(channel.getView().size() == 1)
                        break;
                }
            }
            channel.disconnect(); // will cause thread to terminate anyways
        }
        t=null;
    }


    public void destroy() {
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }

    public void setReceiver(Receiver r) {
        this.receiver=r;
    }

    public Map dumpStats(String parameters) {
        if(parameters != null && parameters.length() > 0) {
            List<String> list=null;
            int index=0;
            String protocol_name=parameters;
            index=protocol_name.indexOf(".");
            if(index > -1) {
                String rest=protocol_name;
                protocol_name=protocol_name.substring(0, index);
                String attrs=rest.substring(index +1); // e.g. "num_sent,msgs,num_received_msgs"
                list=Util.parseStringList(attrs, ",");
            }
            return channel.dumpStats(protocol_name, list);
        }
        else
            return channel.dumpStats();
    }

    public void send(Object destination, byte[] payload, boolean oob) throws Exception {
        Message msg=new Message((Address)destination, null, payload);
        if(oob)
            msg.setFlag(Message.OOB);
        channel.send(msg);
    }


    public void receive(Message msg) {
        Address sender=msg.getSrc();
        byte[] payload=msg.getBuffer();
        if(receiver != null) {
            try {
                receiver.receive(sender, payload);
            }
            catch(Throwable tt) {
                tt.printStackTrace();
            }
        }
    }

    private static boolean isCoordinator(Channel ch) {
        List<Address> members=ch.getView().getMembers();
        Address local_addr=ch.getAddress();
        return !members.isEmpty() && members.get(0).equals(local_addr);
    }


}
