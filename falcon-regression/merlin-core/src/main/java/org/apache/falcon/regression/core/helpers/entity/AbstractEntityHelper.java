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

package org.apache.falcon.regression.core.helpers.entity;

import com.jcraft.jsch.JSchException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.helpers.FalconClientBuilder;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ExecResult;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.ExecUtil;
import org.apache.falcon.regression.core.util.FileUtil;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.HiveUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.TriageResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Abstract class for helper classes. */
public abstract class AbstractEntityHelper {

    private static final Logger LOGGER = Logger.getLogger(AbstractEntityHelper.class);

    //basic properties
    private String qaHost;
    private String hostname = "";
    private String username = "";
    private String password = "";
    private String hadoopLocation = "";
    private String hadoopURL = "";
    private String clusterReadonly = "";
    private String clusterWrite = "";
    private String oozieURL = "";
    private String activeMQ = "";
    private String storeLocation = "";
    private String colo;
    private String allColo;
    private String coloName;
    //hive jdbc
    private String hiveJdbcUrl = "";
    private String hiveJdbcUser = "";
    private String hiveJdbcPassword = "";
    private Connection hiveJdbcConnection;
    //clients
    private OozieClient oozieClient;
    private String hcatEndpoint = "";
    private HCatClient hCatClient;
    private FileSystem hadoopFS;
    //other properties
    private String namenodePrincipal;
    private String hiveMetaStorePrincipal;
    private String identityFile;
    private String serviceUser;
    private String serviceStartCmd;
    private String serviceStopCmd;
    private String serviceStatusMsg;
    private String serviceStatusCmd;

    public AbstractEntityHelper(String prefix) {
        if ((null == prefix) || prefix.isEmpty()) {
            prefix = "";
        } else {
            prefix += ".";
        }
        this.qaHost = Config.getProperty(prefix + "qa_host");
        this.hostname = Config.getProperty(prefix + "hostname");
        this.username = Config.getProperty(prefix + "username", System.getProperty("user.name"));
        this.password = Config.getProperty(prefix + "password", "");
        this.hadoopLocation = Config.getProperty(prefix + "hadoop_location");
        this.hadoopURL = Config.getProperty(prefix + "hadoop_url");
        this.hcatEndpoint = Config.getProperty(prefix + "hcat_endpoint");
        this.clusterReadonly = Config.getProperty(prefix + "cluster_readonly");
        this.clusterWrite = Config.getProperty(prefix + "cluster_write");
        this.oozieURL = Config.getProperty(prefix + "oozie_url");
        this.activeMQ = Config.getProperty(prefix + "activemq_url");
        this.storeLocation = Config.getProperty(prefix + "storeLocation");
        this.allColo = "?colo=" + Config.getProperty(prefix + "colo", "*");
        this.colo = (!Config.getProperty(prefix + "colo", "").isEmpty()) ? "?colo=" + Config
            .getProperty(prefix + "colo") : "";
        this.coloName = this.colo.contains("=") ? this.colo.split("=")[1] : "";
        this.serviceStartCmd =
            Config.getProperty(prefix + "service_start_cmd", "/etc/init.d/tomcat6 start");
        this.serviceStopCmd = Config.getProperty(prefix + "service_stop_cmd",
            "/etc/init.d/tomcat6 stop");
        this.serviceUser = Config.getProperty(prefix + "service_user", null);
        this.serviceStatusMsg = Config.getProperty(prefix + "service_status_msg",
            "Tomcat servlet engine is running with pid");
        this.serviceStatusCmd =
            Config.getProperty(prefix + "service_status_cmd", "/etc/init.d/tomcat6 status");
        this.identityFile = Config.getProperty(prefix + "identityFile",
            System.getProperty("user.home") + "/.ssh/id_rsa");
        this.hadoopFS = null;
        this.oozieClient = null;
        this.namenodePrincipal = Config.getProperty(prefix + "namenode.kerberos.principal", "none");
        this.hiveMetaStorePrincipal = Config.getProperty(
            prefix + "hive.metastore.kerberos.principal", "none");
        this.hiveJdbcUrl = Config.getProperty(prefix + "hive.jdbc.url", "none");
        this.hiveJdbcUser =
            Config.getProperty(prefix + "hive.jdbc.user", System.getProperty("user.name"));
        this.hiveJdbcPassword = Config.getProperty(prefix + "hive.jdbc.password", "");
    }

    /**
     * @param data entity definition
     * @return entity name
     */
    public static String readEntityName(String data) {
        if (data.contains("uri:falcon:feed")) {
            return new FeedMerlin(data).getName();
        } else if (data.contains("uri:falcon:process")) {
            return new ProcessMerlin(data).getName();
        } else {
            return new ClusterMerlin(data).getName();
        }
    }

    public String getActiveMQ() {
        return activeMQ;
    }

    public String getHadoopLocation() {
        return hadoopLocation;
    }

    public String getHadoopURL() {
        return hadoopURL;
    }

    public String getClusterReadonly() {
        return clusterReadonly;
    }

    public String getClusterWrite() {
        return clusterWrite;
    }

    public String getHostname() {
        return hostname;
    }

    public String getPassword() {
        return password;
    }

    public String getStoreLocation() {
        return storeLocation;
    }

    public String getUsername() {
        return username;
    }

    public String getHCatEndpoint() {
        return hcatEndpoint;
    }

    public String getQaHost() {
        return qaHost;
    }

    public String getIdentityFile() {
        return identityFile;
    }

    public String getServiceUser() {
        return serviceUser;
    }

    public String getServiceStopCmd() {
        return serviceStopCmd;
    }

    public String getServiceStartCmd() {
        return serviceStartCmd;
    }

    public String getColo() {
        return colo;
    }

    public String getColoName() {
        return coloName;
    }

    public abstract String getEntityType();

    public String getNamenodePrincipal() {
        return namenodePrincipal;
    }

    public String getHiveMetaStorePrincipal() {
        return hiveMetaStorePrincipal;
    }

    public HCatClient getHCatClient() {
        if (null == this.hCatClient) {
            try {
                this.hCatClient = HCatUtil.getHCatClient(hcatEndpoint, hiveMetaStorePrincipal);
            } catch (HCatException e) {
                Assert.fail("Unable to create hCatClient because of exception:\n"
                    + ExceptionUtils.getStackTrace(e));
            }
        }
        return this.hCatClient;
    }

    public Connection getHiveJdbcConnection() {
        if (null == hiveJdbcConnection) {
            try {
                hiveJdbcConnection =
                    HiveUtil.getHiveJdbcConnection(hiveJdbcUrl, hiveJdbcUser, hiveJdbcPassword, hiveMetaStorePrincipal);
            } catch (ClassNotFoundException | SQLException | InterruptedException | IOException e) {
                Assert.fail("Unable to create hive jdbc connection because of exception:\n"
                    + ExceptionUtils.getStackTrace(e));
            }
        }
        return hiveJdbcConnection;
    }

    public OozieClient getOozieClient() {
        if (null == this.oozieClient) {
            this.oozieClient = OozieUtil.getClient(this.oozieURL);
        }
        return this.oozieClient;
    }

    public FileSystem getHadoopFS() throws IOException {
        if (null == this.hadoopFS) {
            Configuration conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            conf.set("fs.default.name", "hdfs://" + this.hadoopURL);
            this.hadoopFS = FileSystem.get(conf);
        }
        return this.hadoopFS;
    }

    private String createUrl(String... parts) {
        return StringUtils.join(parts, "/");
    }

    public ServiceResponse listEntities(String entityType, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        if (StringUtils.isEmpty(entityType)) {
            entityType = getEntityType();
        }
        LOGGER.info("fetching " + entityType + " list");
        String url = createUrl(this.hostname + URLS.LIST_URL.getValue(), entityType + colo);
        if (StringUtils.isNotEmpty(params)){
            url += colo.isEmpty() ? "?" + params : "&" + params;
        }
        return Util.sendRequest(createUrl(url), "get", null, user);
    }

    public ServiceResponse listAllEntities()
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        return listAllEntities(null, null);
    }

    public ServiceResponse listAllEntities(String params, String user)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        return listEntities(null, (params == null ? "" : params + '&')
            + "numResults=" + Integer.MAX_VALUE, user);
    }

    /**
     * Usual validate entity method.
     */
    public ServiceResponse validateEntity(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return validateEntity(entity.toString(), null);
    }

    /**
     * Use this method directly if specific string definition or another user is required.
     */
    public ServiceResponse validateEntity(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        LOGGER.info("Validating " + getEntityType() + ": \n" + Util.prettyPrintXml(data));
        return Util.sendRequest(createUrl(this.hostname + URLS.VALIDATE_URL.getValue(),
            getEntityType() + colo), "post", data, user);
    }

    /**
     * Method to wrap up entity.getName() invocation.
     */
    public ServiceResponse schedule(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return schedule(entity.getName(), null);
    }

    /**
     * Usual method to schedule an entity.
     */
    public ServiceResponse schedule(String entityName)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return schedule(entityName, null);
    }

    /**
     * Use this method directly if different user is needed.
     */
    public ServiceResponse schedule(String entityName, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.SCHEDULE_URL.getValue(),
            getEntityType(), entityName + colo), "post", user);
    }

    /**
     * Method to wrap up entity.getName() invocation.
     */
    public ServiceResponse delete(Entity entity)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException {
        return delete(entity.getName(), null);
    }

    /**
     * Usual method to delete an entity.
     */
    public ServiceResponse delete(String entityName)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException {
        return delete(entityName, null);
    }

    /**
     * Use this method directly if different user is needed.
     */
    public ServiceResponse delete(String entityName, String user)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.DELETE_URL.getValue(),
            getEntityType(), entityName + colo), "delete", user);
    }

    /**
     * Method to wrap up entity.getName() invocation.
     */
    public ServiceResponse suspend(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return suspend(entity.getName(), null);
    }

    /**
     * Usual method to delete an entity.
     */
    public ServiceResponse suspend(String entityName)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return suspend(entityName, null);
    }

    /**
     * Use this method directly if different user is needed.
     */
    public ServiceResponse suspend(String entityName, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.SUSPEND_URL.getValue(),
            getEntityType(), entityName + colo), "post", user);
    }

    public ServiceResponse resume(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return resume(entity.getName(), null);
    }

    public ServiceResponse resume(String entityName, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.RESUME_URL.getValue(),
            getEntityType(), entityName + colo), "post", user);
    }

    public ServiceResponse getEntityDependencies(String entityName, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.DEPENDENCIES.getValue(),
            getEntityType(), entityName + colo), "get", user);
    }

    /**
     * Usual method to submit an entity.
     */
    public ServiceResponse submitEntity(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return submitEntity(entity.toString(), null);
    }

    /**
     * Use this method directly if another user or non-trivial entity definition is required.
     */
    public ServiceResponse submitEntity(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        LOGGER.info("Submitting " + getEntityType() + ": \n" + Util.prettyPrintXml(data));
        return Util.sendRequest(createUrl(this.hostname + URLS.SUBMIT_URL.getValue(),
            getEntityType() + colo), "post", data, user);
    }

    /**
     * Usual method to submit and schedule an entity.
     */
    public ServiceResponse submitAndSchedule(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return submitAndSchedule(entity, null);
    }

    /**
     * Use this method directly only if another user is needed.
     */
    public ServiceResponse submitAndSchedule(Entity entity, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        LOGGER.info("Submitting " + getEntityType() + ": \n" + Util.prettyPrintXml(entity));
        return Util.sendRequest(createUrl(this.hostname + URLS.SUBMIT_AND_SCHEDULE_URL.getValue(),
            getEntityType()), "post", entity.toString(), user);
    }

    /**
     * Usual getStatus method.
     */
    public ServiceResponse getStatus(String entityName)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getStatus(entityName, null);
    }

    /**
     * Use this method directly only if another user is needed.
     */
    public ServiceResponse getStatus(String entityName, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.STATUS_URL.getValue(),
            getEntityType(), entityName + colo), "get", user);
    }

    /**
     * Use this method to wrap up entity.getName() invocation.
     */
    public ServiceResponse getStatus(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getStatus(entity.getName(), null);
    }

    /**
     * Method to wrap invoking of entity.getName() method.
     */
    public ServiceResponse getEntityDefinition(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getEntityDefinition(entity.getName(), null);
    }

    /**
     * Usual getEntityDefinition method.
     */
    public ServiceResponse getEntityDefinition(String entityName)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getEntityDefinition(entityName, null);
    }

    /**
     * Use this method directly only if another user is needed.
     */
    public ServiceResponse getEntityDefinition(String entityName, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.GET_ENTITY_DEFINITION.getValue(),
            getEntityType(), entityName + colo), "get", user);
    }

    public InstancesResult getRunningInstance(String entityName)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getRunningInstance(entityName, null);
    }

    public InstancesResult getRunningInstance(String entityName, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RUNNING.getValue(), getEntityType(),
            entityName + allColo);
        return (InstancesResult) InstanceUtil.sendRequestProcessInstance(url, user);
    }

    public InstancesResult getProcessInstanceStatus(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceStatus(entityName, params, null);
    }

    public InstancesResult getProcessInstanceStatus(
        String entityName, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_STATUS.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceLogs(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceLogs(entityName, params, null);
    }

    public InstancesResult getProcessInstanceLogs(String entityName, String params,
                                                  String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_LOGS.getValue(), getEntityType(),
            entityName);
        if (StringUtils.isNotEmpty(params)) {
            url += "?";
        }
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceSuspend(
        String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceSuspend(entityName, params, null);
    }

    public InstancesResult getProcessInstanceSuspend(
        String entityName, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_SUSPEND.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    /**
     * Usual updated method.
     */
    public ServiceResponse update(Entity oldEntity, Entity newEntity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return update(oldEntity.toString(), newEntity.toString(), null);
    }

    /**
     * Use this method directly only if another user is needed.
     */
    public ServiceResponse update(String oldEntity, String newEntity, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        LOGGER.info("Updating " + getEntityType() + ": \n" + Util.prettyPrintXml(oldEntity));
        LOGGER.info("To " + getEntityType() + ": \n" + Util.prettyPrintXml(newEntity));
        String url = createUrl(this.hostname + URLS.UPDATE.getValue(), getEntityType(), readEntityName(oldEntity));
        return Util.sendRequest(url + colo, "post", newEntity, user);
    }

    public InstancesResult getProcessInstanceKill(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceKill(entityName, params, null);
    }

    public InstancesResult getProcessInstanceKill(String entityName, String params,
                                                         String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_KILL.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceRerun(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceRerun(entityName, params, null);
    }

    public InstancesResult getProcessInstanceRerun(String entityName, String params,
                                                          String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RERUN.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceResume(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceResume(entityName, params, null);
    }

    public InstancesResult getProcessInstanceResume(String entityName, String params,
                                                           String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RESUME.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public FeedInstanceResult getFeedInstanceListing(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getFeedInstanceListing(entityName, params, null);
    }

    public FeedInstanceResult getFeedInstanceListing(String entityName, String params,
                                                     String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_LISTING.getValue(), getEntityType(),
                entityName, "");
        return (FeedInstanceResult) InstanceUtil
                .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesSummaryResult getInstanceSummary(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_SUMMARY.getValue(), getEntityType(),
            entityName, "");
        return (InstancesSummaryResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, null);
    }

    public List<String> getArchiveInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/archive/" + getEntityType().toUpperCase());
    }

    public List<String> getStoreInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/" + getEntityType().toUpperCase());
    }

    public InstancesResult getInstanceParams(String entityName, String params)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_PARAMS.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, null);
    }

    /**
     * Retrieves instance triage.
     */
    public TriageResult getInstanceTriage(String entityName, String params)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_TRIAGE.getValue(), getEntityType(), entityName);
        return (TriageResult) InstanceUtil.createAndSendRequestProcessInstance(url, params, allColo, null);
    }

    /**
     * Lists all entities which are tagged by a given pipeline.
     * @param pipeline filter
     * @return service response
     * @throws AuthenticationException
     * @throws IOException
     * @throws URISyntaxException
     */
    public ServiceResponse getListByPipeline(String pipeline)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.LIST_URL.getValue() + "/" + getEntityType());
        url += "?filterBy=PIPELINES:" + pipeline;
        return Util.sendRequest(url, "get", null, null);
    }

    /**
     * Submit an entity through falcon client.
     * @param entity entity
     * @throws IOException
     */
    public ExecResult clientSubmit(Entity entity) throws IOException {
        LOGGER.info("Submitting " + getEntityType() + " through falcon client: \n" + Util.prettyPrintXml(entity));
        final String fileName = FileUtil.writeEntityToFile(entity);
        final CommandLine commandLine = FalconClientBuilder.getBuilder()
                .getSubmitCommand(getEntityType(), fileName).build();
        return ExecUtil.executeCommand(commandLine);
    }

    /**
     * Delete an entity through falcon client.
     * @param entityName name of entity to be deleted
     */
    public ExecResult clientDelete(final String entityName, String user) throws IOException {
        LOGGER.info("Deleting " + getEntityType() + ": " + entityName);
        final CommandLine commandLine = FalconClientBuilder.getBuilder(user)
                .getDeleteCommand(getEntityType(), entityName).build();
        return ExecUtil.executeCommand(commandLine);
    }

    /**
     * Retrieves entities summary.
     * @param clusterNameParam compulsory parameter for request
     * @param params list of optional parameters
     * @return entity summary along with its instances.
     */
    public ServiceResponse getEntitySummary(String clusterNameParam, String params)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.ENTITY_SUMMARY.getValue(),
            getEntityType()) +"?cluster=" + clusterNameParam;
        if (StringUtils.isNotEmpty(params)) {
            url += "&" + params;
        }
        return Util.sendRequest(url, "get", null, null);
    }

    /**
     * Get list of all instances of a given entity.
     * @param entityName entity name
     * @param params list of optional parameters
     * @param user user name
     * @return response
     */
    public InstancesResult listInstances(String entityName, String params, String user)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_LIST.getValue(), getEntityType(),
            entityName + colo);
        if (StringUtils.isNotEmpty(params)) {
            url += colo.isEmpty() ? "?" + params : "&" + params;
        }
        return (InstancesResult) InstanceUtil.sendRequestProcessInstance(url, user);
    }

    /**
     * Get list of all dependencies of a given entity.
     * @param entityName entity name
     * @return response
     * @throws URISyntaxException
     * @throws AuthenticationException
     * @throws InterruptedException
     * @throws IOException
     */
    public ServiceResponse getDependencies(String entityName)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException {
        String url = createUrl(this.hostname + URLS.DEPENDENCIES.getValue(), getEntityType(),
            entityName + colo);
        return Util.sendRequest(url, "get", null, null);
    }

    public ServiceResponse touchEntity(Entity entity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return touchEntity(entity.getName(), entity.toString(), null);
    }

    public ServiceResponse touchEntity(String entityName, String entity, String user)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.TOUCH_URL.getValue(), getEntityType(),
                entityName + colo);
        return Util.sendRequest(url, "post", entity, user);
    }

    /**
     * Retrieves entities lineage.
     * @param params list of optional parameters
     * @return entity lineage for the given pipeline.
     */
    public ServiceResponse getEntityLineage(String params)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException {
        String url = createUrl(this.hostname + URLS.ENTITY_LINEAGE.getValue(), colo);
        if (StringUtils.isNotEmpty(params)){
            url += colo.isEmpty() ? "?" + params : "&" + params;
        }
        return Util.sendRequestLineage(createUrl(url), "get", null, null);
    }

    /**
     * Retrieves instance dependencies.
     */
    public InstanceDependencyResult getInstanceDependencies(
            String entityName, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_DEPENDENCIES.getValue(), getEntityType(), entityName, "");
        return (InstanceDependencyResult) InstanceUtil
                .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

}
