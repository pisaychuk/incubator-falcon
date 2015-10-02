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

package org.apache.falcon.regression.core.bundle;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.LateProcess;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A bundle abstraction.
 */
public class Bundle {

    private static final String PRISM_PREFIX = "prism";
    private static ColoHelper prismHelper = new ColoHelper(PRISM_PREFIX);
    private static final Logger LOGGER = Logger.getLogger(Bundle.class);

    private List<ClusterMerlin> clusters;
    private List<FeedMerlin> feeds;
    private ProcessMerlin process;

    public void submitFeed()
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        InterruptedException {
        submitClusters(prismHelper);

        AssertUtil.assertSucceeded(prismHelper.getFeedHelper().submitEntity(feeds.get(0)));
    }

    public void submitAndScheduleFeed() throws Exception {
        submitClusters(prismHelper);

        AssertUtil.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(feeds.get(0)));
    }

    public void submitAndScheduleFeedUsingColoHelper(ColoHelper coloHelper) throws Exception {
        submitFeed();

        AssertUtil.assertSucceeded(coloHelper.getFeedHelper().schedule(feeds.get(0)));
    }

    public void submitAndScheduleAllFeeds()
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        submitClusters(prismHelper);

        for (FeedMerlin feed : feeds) {
            AssertUtil.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(feed));
        }
    }

    public ServiceResponse submitProcess(boolean shouldSucceed) throws JAXBException,
        IOException, URISyntaxException, AuthenticationException, InterruptedException {
        submitClusters(prismHelper);
        submitFeeds(prismHelper);
        ServiceResponse r = prismHelper.getProcessHelper().submitEntity(process);
        if (shouldSucceed) {
            AssertUtil.assertSucceeded(r);
        } else {
            AssertUtil.assertFailed(r);
        }
        return r;
    }

    public void submitFeedsScheduleProcess() throws Exception {
        submitClusters(prismHelper);

        submitFeeds(prismHelper);

        AssertUtil.assertSucceeded(prismHelper.getProcessHelper().submitAndSchedule(process));
    }


    public void submitAndScheduleProcess() throws Exception {
        submitAndScheduleAllFeeds();

        AssertUtil.assertSucceeded(prismHelper.getProcessHelper().submitAndSchedule(process));
    }

    public void submitAndScheduleProcessUsingColoHelper(ColoHelper coloHelper) throws Exception {
        submitProcess(true);

        AssertUtil.assertSucceeded(coloHelper.getProcessHelper().schedule(process));
    }

    public List<ClusterMerlin> getClusters() {
        return clusters;
    }

    public Bundle(ClusterMerlin cluster, List<FeedMerlin> feeds, ProcessMerlin process) {
        this.feeds = feeds;
        this.process = process;
        this.clusters = new ArrayList<>();
        this.clusters.add(cluster);
    }

    public Bundle(Bundle bundle, String prefix) {
        this.feeds = new ArrayList<>();
        for (FeedMerlin feed : bundle.getFeeds()) {
            feeds.add(feed.getClone());
        }
        if (bundle.getProcess() != null) {
            this.process = bundle.getProcess().getClone();
        }
        this.clusters = new ArrayList<>();
        for (ClusterMerlin cluster : bundle.getClusters()) {
            this.clusters.add(BundleUtil.getEnvClusterXML(cluster.getClone(), prefix));
        }
    }

    public Bundle(Bundle bundle, ColoHelper helper) {
        this(bundle, helper.getPrefix());
    }

    public void setClusters(List<ClusterMerlin> pClusters) {
        this.clusters = new ArrayList<>(pClusters);
    }
    /**
     * Unwraps cluster element to string and writes it to bundle.
     *
     * @param cluster      Cluster object to be unwrapped and set into bundle
     */
    public void writeClusterElement(ClusterMerlin cluster) {
        final List<ClusterMerlin> newClusters = new ArrayList<>();
        newClusters.add(cluster);
        setClusters(newClusters);
    }

    /**
     * Wraps bundle cluster in a Cluster object.
     *
     * @return cluster definition in a form of Cluster object
     */
    public ClusterMerlin getClusterElement() {
        return getClusters().get(0);
    }


    public List<String> getClusterNames() {
        List<String> clusterNames = new ArrayList<>();
        for (ClusterMerlin cluster : clusters) {
            clusterNames.add(cluster.getName());
        }
        return clusterNames;
    }

    public List<FeedMerlin> getFeeds() {
        return feeds;
    }

    public void setFeeds(List<FeedMerlin> feeds) {
        this.feeds = feeds;
    }

    public ProcessMerlin getProcess() {
        return process;
    }

    public void setProcess(ProcessMerlin process) {
        this.process = process;
    }

    /**
     * Generates unique entities within a bundle changing their names and names of dependant items
     * to unique.
     */
    public void generateUniqueBundle(Object testClassObject) {
        generateUniqueBundle(testClassObject.getClass().getSimpleName());
    }

    /**
     * Generates unique entities within a bundle changing their names and names of dependant items
     * to unique.
     */
    public void generateUniqueBundle(String prefix) {
        /* creating new names */
        Map<String, String> clusterNameMap = new HashMap<>();
        for (ClusterMerlin clusterMerlin : clusters) {
            clusterNameMap.putAll(clusterMerlin.setUniqueName(prefix));
        }
        Map<String, String> feedNameMap = new HashMap<>();
        for (FeedMerlin feedMerlin : feeds) {
            feedNameMap.putAll(feedMerlin.setUniqueName(prefix));
        }
        /* setting new names in feeds and process */
        for (FeedMerlin feedMerlin : feeds) {
            feedMerlin.renameClusters(clusterNameMap);
        }
        if (process != null) {
            process.setUniqueName(prefix);
            process.renameClusters(clusterNameMap);
            process.renameFeeds(feedNameMap);
        }
    }

    public ServiceResponse submitBundle(ColoHelper helper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {

        submitClusters(helper);

        //lets submit all data first
        submitFeeds(helper);

        return helper.getProcessHelper().submitEntity(getProcess());
    }

    /**
     * Submit all the entities and schedule the process.
     *
     * @param helper helper of prism host
     * @return schedule response or cluster submit response if it fails
     * @throws IOException
     * @throws JAXBException
     * @throws URISyntaxException
     * @throws AuthenticationException
     */
    public ServiceResponse submitFeedsScheduleProcess(ColoHelper helper)
        throws IOException, JAXBException, URISyntaxException,
        AuthenticationException, InterruptedException {
        ServiceResponse submitResponse = submitBundle(helper);
        if (submitResponse.getCode() == 400) {
            return submitResponse;
        }

        //lets schedule the damn thing now :)
        ServiceResponse scheduleResult = helper.getProcessHelper().schedule(getProcess());
        AssertUtil.assertSucceeded(scheduleResult);
        TimeUtil.sleepSeconds(7);
        return scheduleResult;
    }

    /**
     * Sets the only process input.
     *
     * @param startEl its start in terms of EL expression
     * @param endEl its end in terms of EL expression
     */
    public void setProcessInput(String startEl, String endEl) {
        process.setInputFeedWithEl(getInputFeedFromBundle().getName(), startEl, endEl);
    }

    public void setInvalidData() {
        FeedMerlin dataElement = new FeedMerlin(getInputFeedFromBundle());
        String oldLocation = dataElement.getLocations().getLocations().get(0).getPath();
        LOGGER.info("oldlocation: " + oldLocation);
        dataElement.getLocations().getLocations().get(0).setPath(
            oldLocation.substring(0, oldLocation.indexOf('$')) + "invalid/"
                    + oldLocation.substring(oldLocation.indexOf('$')));
        LOGGER.info("new location: " + dataElement.getLocations().getLocations().get(0).getPath());
        setInputFeed(dataElement);
    }

    public void setInputFeed(FeedMerlin newFeed) {
        String inputFeedName = getInputFeedNameFromBundle();
        for (int i = 0; i < feeds.size(); i++) {
            if (new FeedMerlin(feeds.get(i)).getName().equals(inputFeedName)) {
                feeds.set(i, newFeed);
                return;
            }
        }
    }

    public void setFeedValidity(String feedStart, String feedEnd, String feedName) {
        FeedMerlin feedElement = getFeedElement(feedName);
        feedElement.setValidity(feedStart, feedEnd);
        writeFeedElement(feedElement, feedName);
    }

    public int getInitialDatasetFrequency() {
        FeedMerlin dataElement = new FeedMerlin(getInputFeedFromBundle());
        if (dataElement.getFrequency().getTimeUnit() == TimeUnit.hours) {
            return (Integer.parseInt(dataElement.getFrequency().getFrequency())) * 60;
        } else {
            return (Integer.parseInt(dataElement.getFrequency().getFrequency()));
        }
    }

    public Date getStartInstanceProcess(Calendar time) {
        ProcessMerlin processElement = getProcess();
        LOGGER.info("start instance: " + processElement.getInputs().getInputs().get(0).getStart());
        return TimeUtil.getMinutes(processElement.getInputs().getInputs().get(0).getStart(), time);
    }

    public Date getEndInstanceProcess(Calendar time) {
        ProcessMerlin processElement = getProcess();
        LOGGER.info("end instance: " + processElement.getInputs().getInputs().get(0).getEnd());
        LOGGER.info("timezone in getendinstance: " + time.getTimeZone().toString());
        LOGGER.info("time in getendinstance: " + time.getTime());
        return TimeUtil.getMinutes(processElement.getInputs().getInputs().get(0).getEnd(), time);
    }

    public void setDatasetInstances(String startInstance, String endInstance) {
        process.setDatasetInstances(startInstance, endInstance);
    }

    public void setProcessPeriodicity(int frequency, TimeUnit periodicity) {
        process.setPeriodicity(frequency, periodicity);
    }

    public void setProcessInputStartEnd(String start, String end) {
        process.setProcessInputStartEnd(start, end);
    }

    public void setOutputFeedPeriodicity(int frequency, TimeUnit periodicity) {
        FeedMerlin outputFeed = null;
        int datasetIndex;
        for (datasetIndex = 0; datasetIndex < feeds.size(); datasetIndex++) {
            outputFeed = feeds.get(datasetIndex);
            if (outputFeed.getName().equals(process.getOutputs().getOutputs().get(0).getFeed())) {
                break;
            }
        }
        Assert.assertNotNull(outputFeed, "Output feed should be present.");
        outputFeed.setFrequency(new Frequency("" + frequency, periodicity));
        feeds.set(datasetIndex, outputFeed);
        LOGGER.info("modified o/p dataSet is: " + feeds.get(datasetIndex));
    }

    public int getProcessConcurrency() {
        return getProcess().getParallel();
    }

    public void setOutputFeedLocationData(String path) {
        FeedMerlin feedElement = getFeedElement(getOutputFeedNameFromBundle());
        feedElement.setDataLocationPath(path);
        writeFeedElement(feedElement, feedElement.getName());
        LOGGER.info("modified location path dataSet is: " + feedElement);
    }

    public void setProcessConcurrency(int concurrency) {
        process.setParallel((concurrency));
    }

    public void setProcessWorkflow(String wfPath) {
        setProcessWorkflow(wfPath, null);
    }

    public void setProcessWorkflow(String wfPath, EngineType engineType) {
        setProcessWorkflow(wfPath, null, engineType);
    }

    public void setProcessWorkflow(String wfPath, String libPath, EngineType engineType) {
        process.setWorkflow(wfPath, libPath, engineType);
    }

    public FeedMerlin getFeedElement(String feedName) {
        return getFeed(feedName);
    }

    public FeedMerlin getFeed(String feedName) {
        for (FeedMerlin feed : getFeeds()) {
            if (feed.getName().equals(feedName)) {
                return feed;
            }
        }
        return null;
    }

    public void writeFeedElement(FeedMerlin feed, String feedName) {
        feeds.set(feeds.indexOf(getFeed(feedName)), feed);
    }

    public void setInputFeedPeriodicity(int frequency, TimeUnit periodicity) {
        String feedName = getInputFeedNameFromBundle();
        FeedMerlin feedElement = getFeedElement(feedName);
        feedElement.setPeriodicity(frequency, periodicity);
        writeFeedElement(feedElement, feedName);

    }

    public void setInputFeedValidity(String startInstance, String endInstance) {
        String feedName = getInputFeedNameFromBundle();
        this.setFeedValidity(startInstance, endInstance, feedName);
    }

    public void setOutputFeedValidity(String startInstance, String endInstance) {
        String feedName = getOutputFeedNameFromBundle();
        this.setFeedValidity(startInstance, endInstance, feedName);
    }

    public void setInputFeedDataPath(String path) {
        String feedName = getInputFeedNameFromBundle();
        FeedMerlin feedElement = getFeedElement(feedName);
        feedElement.setDataLocationPath(path);
        writeFeedElement(feedElement, feedName);
    }

    public String getFeedDataPathPrefix() {
        FeedMerlin feedElement =
            getFeedElement(getInputFeedNameFromBundle());
        return Util.getPathPrefix(feedElement.getLocations().getLocations().get(0)
            .getPath());
    }

    public void setProcessValidity(String startDate, String endDate) {
        process.setValidity(startDate, endDate);
    }

    public void setProcessLatePolicy(LateProcess lateProcess) {
        process.setLateProcess(lateProcess);
    }

    public void verifyDependencyListing(ColoHelper coloHelper)
        throws InterruptedException, IOException, AuthenticationException, URISyntaxException {
        //display dependencies of process:
        String dependencies = coloHelper.getProcessHelper().getDependencies(
            getProcess().getName()).getEntityList().toString();

        //verify presence
        for (ClusterMerlin cluster : clusters) {
            Assert.assertTrue(dependencies.contains("(cluster) " + cluster.getName()));
        }
        for (FeedMerlin feed : getFeeds()) {
            Assert.assertTrue(dependencies.contains("(feed) " + feed.getName()));
            for (ClusterMerlin cluster : clusters) {
                Assert.assertTrue(coloHelper.getFeedHelper().getDependencies(
                    feed.getName()).getEntityList().toString()
                    .contains("(cluster) " + cluster.getName()));
            }
            Assert.assertFalse(coloHelper.getFeedHelper().getDependencies(
                feed.getName()).getEntityList().toString()
                .contains("(process)" + getProcess().getName()));
        }
    }

    public void addProcessInput(String inputName, String feedName) {
        process.addInputFeed(inputName, feedName);
    }

    public void setProcessName(String newName) {
        process.setName(newName);

    }

    public void setRetry(Retry retry) {
        LOGGER.info("old process: " + process.toPrettyXml());
        process.setRetry(retry);
        LOGGER.info("updated process: " + process.toPrettyXml());
    }

    public void setInputFeedAvailabilityFlag(String flag) {
        String feedName = getInputFeedNameFromBundle();
        FeedMerlin feedElement = getFeedElement(feedName);
        feedElement.setAvailabilityFlag(flag);
        writeFeedElement(feedElement, feedName);
    }

    public void setOutputFeedAvailabilityFlag(String flag) {
        String feedName = getOutputFeedNameFromBundle();
        FeedMerlin feedElement = getFeedElement(feedName);
        feedElement.setAvailabilityFlag(flag);
        writeFeedElement(feedElement, feedName);
    }

    public void setCLusterColo(String colo) {
        ClusterMerlin c = getClusterElement();
        c.setColo(colo);
        writeClusterElement(c);

    }

    public void setClusterInterface(Interfacetype interfacetype, String value) {
        ClusterMerlin c = getClusterElement();
        c.setInterface(interfacetype, value);
        writeClusterElement(c);
    }

    public void setInputFeedTableUri(String tableUri) {
        FeedMerlin feed = getInputFeedFromBundle();
        feed.setTableUri(tableUri);
        writeFeedElement(feed, feed.getName());
    }

    public void setOutputFeedTableUri(String tableUri) {
        FeedMerlin feed = getOutputFeedFromBundle();
        feed.setTableUri(tableUri);
        writeFeedElement(feed, feed.getName());
    }

    public void setCLusterWorkingPath(ClusterMerlin clusterData, String path) {
        clusterData.setWorkingLocationPath(path);
        writeClusterElement(clusterData);
    }


    public void submitClusters(ColoHelper helper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        submitClusters(helper, null);
    }

    public void submitClusters(ColoHelper helper, String user)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        for (ClusterMerlin cluster : this.clusters) {
            AssertUtil.assertSucceeded(helper.getClusterHelper().submitEntity(cluster.toString(), user));
        }
    }

    public void submitFeeds(ColoHelper helper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        for (FeedMerlin feed : this.feeds) {
            AssertUtil.assertSucceeded(helper.getFeedHelper().submitEntity(feed));
        }
    }

    public void addClusterToBundle(ClusterMerlin clusterData, ClusterType type,
                                   String startTime, String endTime) {
        this.clusters.add(clusterData);
        //now to add clusters to feeds
        for (int i = 0; i < feeds.size(); i++) {
            FeedMerlin feedObject = new FeedMerlin(feeds.get(i));
            org.apache.falcon.entity.v0.feed.Cluster cluster =
                new org.apache.falcon.entity.v0.feed.Cluster();
            cluster.setName(clusterData.getName());
            cluster.setValidity(feedObject.getClusters().getClusters().get(0).getValidity());
            cluster.setType(type);
            cluster.setRetention(feedObject.getClusters().getClusters().get(0).getRetention());
            feedObject.getClusters().getClusters().add(cluster);

            feeds.remove(i);
            feeds.add(i, feedObject);

        }

        //now to add cluster to process
        Cluster cluster = new Cluster();
        cluster.setName(clusterData.getName());
        org.apache.falcon.entity.v0.process.Validity v =
            this.process.getClusters().getClusters().get(0).getValidity();
        if (StringUtils.isNotEmpty(startTime)) {
            v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
        }
        if (StringUtils.isNotEmpty(endTime)) {
            v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
        }
        cluster.setValidity(v);
        this.process.getClusters().getClusters().add(cluster);
    }

    public void deleteBundle(ColoHelper helper) {
        try {
            helper.getProcessHelper().delete(getProcess().getName());
        } catch (Exception e) {
            e.getStackTrace();
        }
        for (FeedMerlin dataset : getFeeds()) {
            try {
                helper.getFeedHelper().delete(dataset.getName());
            } catch (Exception e) {
                e.getStackTrace();
            }
        }

        for (ClusterMerlin cluster : this.getClusters()) {
            try {
                helper.getClusterHelper().delete(cluster.getName());
            } catch (Exception e) {
                e.getStackTrace();
            }
        }


    }

    public String getProcessName() {
        return new ProcessMerlin(this.process).getName();
    }

    public void setProcessLibPath(String libPath) {
        this.process.getWorkflow().setLib(libPath);
    }

    public void setProcessTimeOut(int magnitude, TimeUnit unit) {
        this.process.setTimeOut(magnitude, unit);
    }

    public static void submitCluster(Bundle... bundles)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        for (Bundle bundle : bundles) {
            ServiceResponse r =
                prismHelper.getClusterHelper().submitEntity(bundle.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());
        }
    }

    /**
     * Generates unique entities definitions: clusters, feeds and process, populating them with
     * desired values of different properties.
     *
     * @param numberOfClusters number of clusters on which feeds and process should run
     * @param numberOfInputs number of desired inputs in process definition
     * @param numberOfOptionalInput how many inputs should be optional
     * @param inputBasePaths base data path for inputs
     * @param numberOfOutputs number of outputs
     * @param startTime start of feeds and process validity on every cluster
     * @param endTime end of feeds and process validity on every cluster
     */
    public void generateRequiredBundle(int numberOfClusters, int numberOfInputs,
                                       int numberOfOptionalInput,
                                       String inputBasePaths, int numberOfOutputs, String startTime,
                                       String endTime) {
        //generate and set clusters
        ClusterMerlin newCluster = getClusters().get(0);
        List<ClusterMerlin> newClusters = new ArrayList<>();
        final String clusterName = newCluster.getName();
        for (int i = 0; i < numberOfClusters; i++) {
            newCluster.setName(clusterName + i);
            newClusters.add(i, newCluster.getClone());
        }
        setClusters(newClusters);

        //generate and set newFeeds
        List<FeedMerlin> newFeeds = new ArrayList<>();
        for (int i = 0; i < numberOfInputs; i++) {
            final FeedMerlin feed = getFeeds().get(0).getClone();
            feed.setName(feed.getName() + "-input" + i);
            feed.setFeedClusters(newClusters, inputBasePaths + "/input" + i, startTime, endTime);
            newFeeds.add(feed);
        }
        for (int i = 0; i < numberOfOutputs; i++) {
            final FeedMerlin feed = getFeeds().get(0).getClone();
            feed.setName(feed.getName() + "-output" + i);
            feed.setFeedClusters(newClusters, inputBasePaths + "/output" + i,  startTime, endTime);
            newFeeds.add(feed);
        }
        setFeeds(newFeeds);

        //add clusters and feed to process
        getProcess().setProcessClusters(newClusters, startTime, endTime);
        getProcess().setProcessFeeds(newFeeds, numberOfInputs,
            numberOfOptionalInput, numberOfOutputs);
    }

    public void submitAndScheduleBundle(ColoHelper helper, boolean checkSuccess)
        throws IOException, JAXBException, URISyntaxException, AuthenticationException,
            InterruptedException {

        for (int i = 0; i < getClusters().size(); i++) {
            ServiceResponse r;
            r = helper.getClusterHelper().submitEntity(getClusters().get(i));
            if (checkSuccess) {
                AssertUtil.assertSucceeded(r);
            }
        }
        for (int i = 0; i < getFeeds().size(); i++) {
            ServiceResponse r = helper.getFeedHelper().submitAndSchedule(getFeeds().get(i));
            if (checkSuccess) {
                AssertUtil.assertSucceeded(r);
            }
        }
        ServiceResponse r = helper.getProcessHelper().submitAndSchedule(getProcess());
        if (checkSuccess) {
            AssertUtil.assertSucceeded(r);
        }
    }

    /**
     * Changes names of process inputs.
     *
     * @param names desired names of inputs
     */
    public void setProcessInputNames(String... names) {
        this.process.setInputNames(names);
    }

    /**
     * Adds optional property to process definition.
     */
    public void addProcessProperty(String propName, String propValue) {
        this.process.withProperty(propName, propValue);
    }

    /**
     * Sets partition for each input, according to number of supplied partitions.
     *
     * @param partition partitions to be set
     */
    public void setProcessInputPartition(String... partition) {
        this.process.setInputPartition(partition);
    }

    /**
     * Sets name(s) of the process output(s).
     *
     * @param names new names of the outputs
     */
    public void setProcessOutputNames(String... names) {
        this.process.setOutputNames(names);
    }

    public void addInputFeedToBundle(String feedRefName, FeedMerlin feed) {
        this.getFeeds().add(feed);
        this.process.addInputFeed(feedRefName, feed.getName());
    }

    public void addOutputFeedToBundle(String feedRefName, FeedMerlin feed) {
        this.getFeeds().add(feed);
        this.process.addOutputFeed(feedRefName, feed.getName());
    }

    public void setProcessProperty(String property, String value) {
        this.process.withProperty(property, value);
    }

    public String getDatasetPath() {
        FeedMerlin dataElement = new FeedMerlin(getInputFeedFromBundle());
        return dataElement.getLocations().getLocations().get(0).getPath();
    }

    public FeedMerlin getInputFeedFromBundle() {
        return getFeed(this.process.getInputs().getInputs().get(0).getFeed());
    }

    public FeedMerlin getOutputFeedFromBundle() {
        return getFeed(this.process.getOutputs().getOutputs().get(0).getFeed());
    }

    public String getOutputFeedNameFromBundle() {
        return getOutputFeedFromBundle().getName();
    }

    public String getInputFeedNameFromBundle() {
        return getInputFeedFromBundle().getName();
    }

    /**
     * Sets process pipelines.
     * @param pipelines proposed pipelines
     */
    public void setProcessPipeline(String... pipelines){
        this.process.setPipelineTag(pipelines);
    }

    /**
     * Set ACL of bundle's cluster.
     */
    public void setCLusterACL(String owner, String group, String permission) {
        ClusterMerlin clusterMerlin = getClusterElement();
        clusterMerlin.setACL(owner, group, permission);
        writeClusterElement(clusterMerlin);

    }

    /**
     * Set ACL of bundle's input feed.
     */
    public void setInputFeedACL(String owner, String group, String permission) {
        String feedName = getInputFeedNameFromBundle();
        FeedMerlin feedMerlin = getFeedElement(feedName);
        feedMerlin.setACL(owner, group, permission);
        writeFeedElement(feedMerlin, feedName);
    }

    /**
     * Set ACL of bundle's process.
     */
    public void setProcessACL(String owner, String group, String permission) {
        this.process.setACL(owner, group, permission);
    }

    /**
     * Set custom tags for a process. Key-value pairs are valid.
     */
    public void setProcessTags(String value) {
        this.process.setTags(value);
    }


    public static int runFalconCLI(List<String> args) throws Exception {
        args.add(1, "-url");
        args.add(2, prismHelper.getClusterHelper().getHostname());
        LOGGER.info("Going to run falcon jar with args: " + args);
        return new FalconCLI().run(args.toArray(new String[]{}));
    }
}
