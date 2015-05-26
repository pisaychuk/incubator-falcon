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

package org.apache.falcon.regression.ui.search;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.RecipeMerlin;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.UIAssert;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.ui.Select;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Page object of the Mirror creation page. */
public class MirrorWizardPage extends AbstractSearchPage {

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "formPage")
    })
    private WebElement mirrorBox;

    public MirrorWizardPage(WebDriver driver) {
        super(driver);
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(mirrorBox, "Mirror box");
    }

    private WebElement getSrcPath() {
        return driver.findElement(By.name("sourceClusterPathInput"));
    }

    private WebElement getTgtPath() {
        return driver.findElement(By.name("targetClusterPathInput"));
    }

    public void setName(String name) {
        clearAndSetByNgModel("UIModel.name", name);
    }

    public void setTags(List<String> tags) {
        //TODO add code here
    }

    public void setMirrorType(FalconCLI.RecipeOperation recipeOperation) {
        switch (recipeOperation) {
            case HDFS_REPLICATION:
                driver.findElement(By.xpath("//button[contains(.,'File System')]")).click();
                break;
            case HIVE_DISASTER_RECOVERY:
                driver.findElement(By.xpath("//button[contains(.,'HIVE')]")).click();
                break;
        }
    }

    public void setSrcName(String clusterName) {
        selectNgModelByVisibleText("UIModel.source.cluster", clusterName);
    }

    public void setSrcPath(String srcPath) {
        final WebElement srcPathElement = getSrcPath();
        clearAndSet(srcPathElement, srcPath);
    }

    public void setTgtName(String clusterName) {
        selectNgModelByVisibleText("UIModel.target.cluster", clusterName);
    }

    public void setTgtPath(String srcPath) {
        final WebElement tgtPathElement = getTgtPath();
        clearAndSet(tgtPathElement, srcPath);
    }

    public void setReplication(RecipeMerlin recipeMerlin) {
        if (StringUtils.isNotEmpty(recipeMerlin.getSourceTable())) {
            clickById("targetHIVETablesRadio");
            clearAndSetByNgModel("UIModel.source.hiveDatabase", recipeMerlin.getSourceDb());
            clearAndSetByNgModel("UIModel.source.hiveTables", recipeMerlin.getSourceTable());
        } else {
            clickById("targetHIVEDatabaseRadio");
            clearAndSetByNgModel("UIModel.source.hiveDatabases", recipeMerlin.getSourceDb());
        }
    }


    public void setStartTime(String validityStartStr) {
        final DateTime startDate = TimeUtil.oozieDateToDate(validityStartStr);

        clearAndSetByNgModel("UIModel.validity.start", DateTimeFormat.forPattern("MM/dd/yyyy").print(startDate));
        final WebElement startTimeBox = driver.findElement(By.className("startTimeBox"));
        final List<WebElement> startHourAndMinute = startTimeBox.findElements(By.tagName("input"));
        final WebElement hourText = startHourAndMinute.get(0);
        final WebElement minuteText = startHourAndMinute.get(1);
        clearAndSet(hourText, DateTimeFormat.forPattern("hh").print(startDate));
        clearAndSet(minuteText, DateTimeFormat.forPattern("mm").print(startDate));
        final WebElement amPmButton = startTimeBox.findElement(By.tagName("button"));
        if (!amPmButton.getText().equals(DateTimeFormat.forPattern("a").print(startDate))) {
            amPmButton.click();
        }
    }

    public void setEndTime(String validityEndStr) {
        final DateTime validityEnd = TimeUtil.oozieDateToDate(validityEndStr);

        clearAndSetByNgModel("UIModel.validity.end", DateTimeFormat.forPattern("MM/dd/yyyy").print(validityEnd));
        final WebElement startTimeBox = driver.findElement(By.className("endTimeBox"));
        final List<WebElement> startHourAndMinute = startTimeBox.findElements(By.tagName("input"));
        final WebElement hourText = startHourAndMinute.get(0);
        final WebElement minuteText = startHourAndMinute.get(1);
        clearAndSet(hourText, DateTimeFormat.forPattern("hh").print(validityEnd));
        clearAndSet(minuteText, DateTimeFormat.forPattern("mm").print(validityEnd));
        final WebElement amPmButton = startTimeBox.findElement(By.tagName("button"));
        if (!amPmButton.getText().equals(DateTimeFormat.forPattern("a").print(validityEnd))) {
            amPmButton.click();
        }
    }

    public void toggleAdvancedOptions() {
        final WebElement advanceOption = driver.findElement(By.xpath("//h4[contains(.,'Advanced options')]"));
        advanceOption.click();
    }


    public void setFrequency(Frequency frequency) {
        clearAndSetByNgModel("UIModel.frequency.number", frequency.getFrequencyAsInt() + "");
        selectNgModelByVisibleText("UIModel.frequency.unit", frequency.getTimeUnit().name().toLowerCase());
    }

    public void setDistCpMaxMaps(String distCpMaxMaps) {
        clearAndSetByNgModel("UIModel.allocation.hive.maxMapsDistcp", distCpMaxMaps);
    }


    public void setReplicationMaxMaps(String replicationMaxMaps) {
        clearAndSetByNgModel("UIModel.allocation.hive.maxMapsMirror", replicationMaxMaps);
    }

    public void setMaxEvents(String maxEvents) {
        clearAndSetByNgModel("UIModel.allocation.hive.maxMapsEvents", maxEvents);
    }

    public void setMaxBandwidth(String maxBandWidth) {
        clearAndSetByNgModel("UIModel.allocation.hive.maxBandwidth", maxBandWidth);
    }


    public void setSourceInfo(ClusterMerlin srcCluster) {
        clearAndSetByNgModel("UIModel.hiveOptions.source.stagingPath", srcCluster.getLocation("staging"));
        clearAndSetByNgModel("UIModel.hiveOptions.source.hiveServerToEndpoint",
            srcCluster.getInterfaceEndpoint(Interfacetype.REGISTRY));
    }

    public void setTargetInfo(ClusterMerlin tgtCluster) {
        clearAndSetByNgModel("UIModel.hiveOptions.target.stagingPath", tgtCluster.getLocation("staging"));
        clearAndSetByNgModel("UIModel.hiveOptions.target.hiveServerToEndpoint",
            tgtCluster.getInterfaceEndpoint(Interfacetype.REGISTRY));
    }

    public void setRetry(Retry retry) {
        selectNgModelByVisibleText("UIModel.retry.policy", retry.getPolicy().toString().toUpperCase());
        clearAndSetByNgModel("UIModel.retry.delay.number", retry.getDelay().getFrequencyAsInt() + "");
        selectNgModelByVisibleText("UIModel.retry.delay.unit", retry.getDelay().getTimeUnit().name().toLowerCase());
        clearAndSetByNgModel("UIModel.retry.attempts", retry.getAttempts() + "");
    }


    public void setAcl(ACL acl) {
        clearAndSetByNgModel("UIModel.acl.owner", acl.getOwner());
        clearAndSetByNgModel("UIModel.acl.group", acl.getGroup());
        clearAndSetByNgModel("UIModel.acl.permissions", acl.getPermission());
    }

    public void next() {
        final WebElement nextButton = driver.findElement(By.xpath("//button[contains(.,'Next')]"));
        nextButton.click();
    }

    public void cancel() {
        driver.findElement(By.xpath("//a[contains(.,'Cancel')]"));
    }

    public void save() {
        final WebElement saveButton = driver.findElement(By.xpath("//button[contains(.,'Save')]"));
        UIAssert.assertDisplayed(saveButton, "Save button in not displayed.");
        saveButton.click();
    }

    public ClusterBlock getSourceBlock() {
        return new ClusterBlock("Source");
    }

    public ClusterBlock getTargetBlock() {
        return new ClusterBlock("Target");
    }

    /**
     * Block of source or target cluster with parameters.
     */
    public final class ClusterBlock {
        private final WebElement mainBlock;
        private final WebElement runHereButton;

        private ClusterBlock(String type) {
            mainBlock = driver.findElement(By.xpath("//h3[contains(.,'" + type + "')]/.."));
            runHereButton = mainBlock.findElement(By.id("runJobOn" + type + "Radio"));
        }

        public Set<Location> getAvailableLocationTypes() {
            List<WebElement> inputs = getLocationBox().findElements(By.xpath(".//input"));
            Set<Location> result = EnumSet.noneOf(Location.class);
            for (WebElement input : inputs) {
                result.add(Location.getByInput(input));
            }
            return result;
        }

        public Location getSelectedLocationType() {
            WebElement selected = getLocationBox()
                .findElement(By.xpath("//input[contains(@class,'ng-valid-parse')]"));
            return Location.getByInput(selected);
        }

        public void setLocationType(Location type) {
            getLocationBox().findElement(By.xpath(
                String.format(".//input[translate(@value,'azures','AZURES')='%s']", type.toString()))).click();
        }

        public void selectRunHere() {
            runHereButton.click();
        }

        public Set<String> getAvailableClusters() {
            Select select = new Select(mainBlock.findElement(By.tagName("select")));
            Set<String> clusters = new TreeSet<>();
            for (WebElement option : select.getOptions()) {
                String cluster = option.getText();
                if (!cluster.equals("-Select cluster-")) {
                    clusters.add(cluster);
                }
            }
            return clusters;
        }

        public boolean isRunHereSelected() {
            return runHereButton.getAttribute("class").contains("ng-valid-parse");
        }

        public boolean isRunHereAvailable() {
            return runHereButton.getAttribute("disabled") == null;
        }


        private WebElement getLocationBox() {
            return mainBlock.findElement(By.className("locationBox"));
        }



    }

    /**
     * Types of source/target location.
     */
    public enum Location {
        HDFS,
        AZURE,
        S3;

        private static Location getByInput(WebElement input) {
            return Location.valueOf(input.getAttribute("value").trim().toUpperCase());
        }

    }

}
