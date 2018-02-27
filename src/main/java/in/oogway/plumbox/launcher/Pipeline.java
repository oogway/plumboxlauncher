package in.oogway.plumbox.launcher;

import java.util.ArrayList;
import java.util.Arrays;

public class Pipeline {
    public String stages;
    private String[] _stages;

    public Pipeline(String stages) {
        this.stages = stages;
        _stages = stages.split(",");
    }

    public String[] getStages() {
        return _stages;
    }

    @Override
    public String toString() {
        return "Pipeline{" +
                "stages='" + stages + '\'' +
                ", _stages=" + Arrays.toString(_stages) +
                '}';
    }

    public ArrayList<Transformer> inflate() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        ArrayList<Transformer> objs = new ArrayList<>();
        for (String className: getStages()) {
            ExtensionLoader<Transformer> loader = new ExtensionLoader<Transformer>();
            Transformer t = loader.LoadClass("", "nl.changer.jobs.ActionsTransformation", Transformer.class);
            //Class act = Class.forName(className.trim());
            //objs.add((Transformer) act.newInstance());
            objs.add(t);
        }
        return objs;
    }
}
