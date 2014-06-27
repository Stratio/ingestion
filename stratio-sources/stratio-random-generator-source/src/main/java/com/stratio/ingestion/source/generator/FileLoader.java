package com.stratio.ingestion.source.generator;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.*;

public class FileLoader {
    private static HashMap<String, FileInfo> files = new HashMap<>();
    private static Random rand = new Random();

    public static String getRandomElementFromFile(String filePath) throws URISyntaxException, FileNotFoundException {
        FileInfo fileInfo;
        if (fileHasBeenPreviouslyLoaded(filePath)) {
            fileInfo = files.get(filePath);
        }
        else {
            File fileFromFileSystem = new File(ClassLoader.getSystemResource(filePath).toURI());
            fileInfo = new FileInfo(fileFromFileSystem, generateListOfWords(fileFromFileSystem));
            files.put(filePath, fileInfo);
        }
        return getRandomString(fileInfo.getFileWords());
    }

    private static boolean fileHasBeenPreviouslyLoaded(String filePath) {
        return files.containsKey(filePath);
    }

    private static String getRandomString(List<String> words) {
        int randomElement = rand.nextInt(words.size());
        return words.get(randomElement);
    }

    private static List<String> generateListOfWords(File file) throws FileNotFoundException {
        Scanner scanner = new Scanner(file);
        List<String> listOfWords = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            listOfWords.add(line);
        }
        scanner.close();
        return listOfWords;
    }
}
