package in.oogway.plumbox.launcher.storage;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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

