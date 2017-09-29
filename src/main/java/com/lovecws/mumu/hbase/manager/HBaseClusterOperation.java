package com.lovecws.mumu.hbase.manager;

import com.lovecws.mumu.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase集群管理
 * @date 2017-09-28 16:27
 */
public class HBaseClusterOperation {

    private static final Logger log = Logger.getLogger(HBaseClusterOperation.class);
    private HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();

    /**
     * 获取集群状态
     */
    public void clusterStatus() {
        Admin admin = hBaseConfiguration.admin();
        try {
            ClusterStatus clusterStatus = admin.getClusterStatus();
            log.info(clusterStatus);
            log.info("servers count:" + clusterStatus.getServersSize());
            Collection<ServerName> servers = clusterStatus.getServers();
            for (ServerName serverName : servers) {
                log.info("region server:" + serverName);
                ServerLoad serverLoad = clusterStatus.getLoad(serverName);
                log.info("serverLoad:" + serverLoad);
                Map<byte[], RegionLoad> regionsLoad = serverLoad.getRegionsLoad();
                for (final RegionLoad regionLoad : regionsLoad.values()) {
                    log.info("regionLoad:" + regionLoad);
                }
            }
            log.info("region count:" + clusterStatus.getRegionsCount());
            log.info("hbase version:" + clusterStatus.getHBaseVersion());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
