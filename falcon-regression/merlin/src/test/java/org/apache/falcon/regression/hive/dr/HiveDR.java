/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.hive.dr;

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.NotifyingAssert;
import org.apache.falcon.regression.core.util.HiveAssert;
import org.apache.falcon.regression.core.util.HiveUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Hive DR Testing.
 */
public class HiveDR extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(HiveDR.class);
    private final ColoHelper cluster = servers.get(0);
    private final ColoHelper cluster2 = servers.get(1);
    private final FileSystem clusterFS = serverFS.get(0);
    private final FileSystem clusterFS2 = serverFS.get(1);
    private final OozieClient clusterOC = serverOC.get(0);
    private final String baseTestHDFSDir = baseHDFSDir + "/HiveDR/";
    private HCatClient clusterHC;
    private HCatClient clusterHC2;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        clusterHC = cluster.getClusterHelper().getHCatClient();
        clusterHC2 = cluster2.getClusterHelper().getHCatClient();
    }

    @Test
    public void dataGeneration() throws Exception {
        final Connection connection = cluster.getClusterHelper().getHiveJdbcConnection();
        HiveUtil.runSql(connection, "show tables");
        HiveUtil.runSql(connection, "drop database if exists hdr_sdb1 cascade");
        HiveUtil.runSql(connection, "create database hdr_sdb1");
        HiveUtil.runSql(connection, "use hdr_sdb1");
        HiveObjectCreator.createVanillaTable(connection);
        HiveObjectCreator.createPartitionedTable(connection);
        HiveObjectCreator.createExternalTable(connection, clusterFS,
            baseTestHDFSDir + "click_data/");

        final Connection connection2 = cluster2.getClusterHelper().getHiveJdbcConnection();
        HiveUtil.runSql(connection2, "show tables");
        HiveUtil.runSql(connection2, "drop database if exists hdr_tdb1 cascade");
        HiveUtil.runSql(connection2, "create database hdr_tdb1");
        HiveUtil.runSql(connection2, "use hdr_tdb1");
        HiveObjectCreator.createVanillaTable(connection2);
        HiveObjectCreator.createPartitionedTable(connection2);
        HiveObjectCreator.createExternalTable(connection2, clusterFS2,
            baseTestHDFSDir + "click_data/");

        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase("hdr_sdb1"),
            cluster2, clusterHC2.getDatabase("hdr_tdb1"), new NotifyingAssert(true)
        ).assertAll();

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable("hdr_sdb1", "click_data"),
            cluster2, clusterHC2.getTable("hdr_tdb1", "click_data"), new NotifyingAssert(true)
        ).assertAll();

    }

    @Test
    public void assertionTest() throws Exception {
        HiveAssert.assertTableEqual(
            cluster, clusterHC.getTable("default", "hcatsmoke10546"),
            cluster2, clusterHC2.getTable("default", "hcatsmoke10548"), new SoftAssert()
        ).assertAll();
        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase("default"), cluster2,
            clusterHC2.getDatabase("default"), new SoftAssert()
        ).assertAll();
    }

    @Test
    public void dynamicPartitionsTest() throws SQLException, IOException {
        final Connection connection = cluster.getClusterHelper().getHiveJdbcConnection();
        HiveUtil.runSql(connection, "show tables");
        HiveUtil.runSql(connection, "drop database if exists hdr_sdb1 cascade");
        HiveUtil.runSql(connection, "create database hdr_sdb1");
        HiveUtil.runSql(connection, "use hdr_sdb1");
        //create table with static partitions
        createPartitionedTable(connection, "global_store_sales", false);

        final Connection connection2 = cluster2.getClusterHelper().getHiveJdbcConnection();
        HiveUtil.runSql(connection2, "show tables");
        HiveUtil.runSql(connection2, "drop database if exists hdr_tdb1 cascade");
        HiveUtil.runSql(connection2, "create database hdr_tdb1");
        HiveUtil.runSql(connection2, "use hdr_tdb1");
        //create table with dynamic partitions
        createPartitionedTable(connection2, "global_store_sales", true);

        //check that both tables are equal
        HiveAssert.assertTableEqual(
            cluster, clusterHC.getTable("hdr_sdb1", "global_store_sales"),
            cluster2, clusterHC2.getTable("hdr_tdb1", "global_store_sales"), new SoftAssert()
        ).assertAll();
    }

    private void createPartitionedTable(Connection connection, String tableName,
                                        boolean dynamic) throws SQLException {
        String [][] partitions = {
            {"us", "Kansas", },
            {"us", "California", },
            {"au", "Queensland", },
            {"au", "Victoria", },
        };
        //create table
        HiveUtil.runSql(connection, "drop table " + tableName);
        HiveUtil.runSql(connection, "create table " + tableName
            + "(customer_id string, item_id string, quantity float, price float, time timestamp) "
            + "partitioned by (country string, state string)");
        //provide data
        String query;
        if (dynamic) {
            //disable strict mode, thus both partitions can be used as dynamic
            HiveUtil.runSql(connection, "set hive.exec.dynamic.partition.mode=nonstrict");
            query = "insert into table " + tableName + " partition"
                + "(country, state) values('c%3$s', 'i%3$s', '%3$s', '%3$s', "
                + "'2001-01-01 01:01:0%3$s', '%1$s', '%2$s')";
        } else {
            query = "insert into table " + tableName + " partition"
                + "(country = '%1$s', state = '%2$s') values('c%3$s', 'i%3$s', '%3$s', '%3$s', "
                + "'2001-01-01 01:01:0%3$s')";
        }
        for(int i = 0 ; i < partitions.length; i++){
            HiveUtil.runSql(connection, String.format(query, partitions[i][0], partitions[i][1], i+1));
        }
        HiveUtil.runSql(connection, "select * from global_store_sales");
    }
}
