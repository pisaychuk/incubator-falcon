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

import org.apache.falcon.regression.core.util.UIAssert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.ui.Select;

import java.util.List;

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

    private WebElement getName() {
        return driver.findElement(By.name("nameInput"));
    }

    public void setName(String name) {
        final WebElement srcNameElement = getName();
        srcNameElement.clear();
        srcNameElement.sendKeys(name);
    }

    public void setTags(List<String> tags) {

    }

    private Select getSrcClusterName() {
        return new Select(driver.findElement(By.id("sourceClusterSelect")));
    }

    public void setSrcName(String clusterName) {
        getSrcClusterName().selectByVisibleText(clusterName);
    }

    private WebElement getSrcPath() {
        return driver.findElement(By.name("sourceClusterPathInput"));
    }

    public void setSrcPath(String srcPath) {
        final WebElement srcPathElement = getSrcPath();
        srcPathElement.clear();
        srcPathElement.sendKeys(srcPath);
    }

    private Select getTgtClusterName() {
        return new Select(driver.findElement(By.id("targetClusterSelect")));
    }

    public void setTgtName(String name) {
        getTgtClusterName().selectByVisibleText(name);
    }

    private WebElement getTgtPath() {
        return driver.findElement(By.name("targetClusterPathInput"));
    }

    public void setTgtPath(String srcPath) {
        final WebElement tgtPathElement = getTgtPath();
        tgtPathElement.clear();
        tgtPathElement.sendKeys(srcPath);
    }

}
