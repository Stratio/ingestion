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
