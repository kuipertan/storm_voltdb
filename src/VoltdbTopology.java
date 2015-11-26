import java.io.FileInputStream;

import java.util.Properties;


import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.BrokerHosts;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;


public class VoltdbTopology {
	
	public Properties prop = new Properties();
	
	void initConfig(){
		FileInputStream in;
		try {
			in = new FileInputStream("config.properties");
			prop.load(in);
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		if(prop.containsKey("NUM_OF_THREAD")){  
	        this.threadNum = Integer.valueOf(prop.getProperty("NUM_OF_THREAD"));  
	    }
	    */
				
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		VoltdbTopology tpl = new VoltdbTopology();
		tpl.initConfig();
		
		String spout_kafka_parl     = tpl.prop.getProperty("spout_kafka_parl");
        String bolt_vdb_parl 		= tpl.prop.getProperty("bolt_voltdb_parl");
        //String bolt_customer_cnt_parl = tpl.prop.getProperty("bolt_customer_cnt_parl");
        //String bolt_pvuv_parl         = tpl.prop.getProperty("bolt_pvuv_parl");
        String spout_kafka_pending      = tpl.prop.getProperty("max_spout_pending");
        String num_workers            = tpl.prop.getProperty("num_workers");
        String num_ackers             = tpl.prop.getProperty("num_ackers");
        String topic				= tpl.prop.getProperty("topic");

        //String sleep_time             = tpl.prop.getProperty("sleep_time");

        if (spout_kafka_parl    == null ||
            num_workers         == null ||
            bolt_vdb_parl 		== null ||
            num_ackers          == null ||
            topic          		== null ||
            spout_kafka_pending == null ) {

            System.out.println("parameters configured in config.properties is not complete.");
            return;
        }

        TopologyBuilder builder = new TopologyBuilder();
        
        String zks = tpl.prop.getProperty("zks");
        String zkpath = tpl.prop.getProperty("zkpath");
        String zkroot = tpl.prop.getProperty("zkRoot");
        
        BrokerHosts brokerHosts = new ZkHosts(zks, zkpath);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkroot, topic);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = true;
        
        String spout_id = "kafka_spout-"+topic;
        
        builder.setSpout(spout_id, new KafkaSpout(spoutConf), Integer.parseInt(spout_kafka_parl));
        builder.setBolt("vdb_bolt-"+topic, new VoltdbBolt(), Integer.parseInt(bolt_vdb_parl)).shuffleGrouping(spout_id);

        Config conf = new Config();
        conf.setMaxSpoutPending(Integer.parseInt(spout_kafka_pending));
        conf.setNumWorkers(Integer.parseInt(num_workers));
        conf.setNumAckers(Integer.parseInt(num_ackers));

        String topology_name = "Voltdb_topology-" + topic;
        StormSubmitter.submitTopology(topology_name, conf, builder.createTopology());
	}

}
