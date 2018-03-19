package in.oogway.plumbox.launcher;

import in.oogway.plumbox.transformer.Transformer;

import java.util.ArrayList;

public class Pipeline {
    public String[] stages;

    public Pipeline(String[] stages) {
        this.stages = stages;
    }

    public Transformer[] inflate() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        ArrayList<Transformer> objs = new ArrayList<>();
        for (String className: stages) {
            Class act = Class.forName(className.trim());
            objs.add((Transformer) act.newInstance());
        }
        return objs.toArray(new Transformer[0]);
    }
}
