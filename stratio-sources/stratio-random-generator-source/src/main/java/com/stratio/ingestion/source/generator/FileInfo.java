/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.source.generator;

import java.io.File;
import java.util.List;

public class FileInfo {
    private File fileContent;
    private List<String> fileWords;

    public FileInfo(File fileContent, List<String> fileWords) {
        this.fileContent = fileContent;
        this.fileWords = fileWords;
    }

    public File getFileContent() {
        return fileContent;
    }

    public void setFileContent(File fileContent) {
        this.fileContent = fileContent;
    }

    public List<String> getFileWords() {
        return fileWords;
    }

    public void setFileWords(List<String> fileWords) {
        this.fileWords = fileWords;
    }

}
