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
            File fileFromFileSystem = new File(filePath);
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
