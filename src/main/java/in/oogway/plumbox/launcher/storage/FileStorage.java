package in.oogway.plumbox.launcher.storage;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class FileStorage implements LauncherStorageDriver {
    private final File dirFile;
    private String dir;

    public FileStorage() throws IOException {
        this(UUID.randomUUID().toString());
    }

    public FileStorage(String dirname) throws IOException {
        dir = dirname;
        dirFile = new File(dir);
        FileUtils.forceMkdir(dirFile);
    }

    public void deleteRecursive() {
        for (File file: dirFile.listFiles()) {
            file.delete();
        }
    }

    @Override
    public void write(String filename, String json) {
        File file = new File(dir, filename);
        try {
            FileUtils.writeStringToFile(file, json);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String read(String filename) {
        try {
            return FileUtils.readFileToString(new File(dir, filename));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static List<Path> listFiles(String rootDirectory) {
        List<Path> files = new ArrayList<>();
        listFiles(rootDirectory, files);

        return files;
    }

    private static void listFiles(String path, List<Path> collectedFiles) {
        File root = new File(path);
        File[] files = root.listFiles();

        if (files == null) {
            return;
        }

        for (File file : files) {
            if (file.isDirectory()) {
                listFiles(file.getAbsolutePath(), collectedFiles);
            } else {
                collectedFiles.add(file.toPath());
            }
        }
    }

    @Override
    public Set<String> getAllKeys(String pattern) {
        File[] files = dirFile.listFiles((d, name) -> name.startsWith(pattern));
        ArrayList<String> selected = new ArrayList<String>(){};
        for (File f: files) {
            selected.add(f.getName());
        }
        return new HashSet<String>(selected);
    }
}

