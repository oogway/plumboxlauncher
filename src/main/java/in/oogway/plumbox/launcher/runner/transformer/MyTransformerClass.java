package in.oogway.plumbox.launcher.runner.transformer;

public class MyTransformerClass implements Transformer {
    @Override
    public void run() {
        System.out.println("Transformation class obtained. " + this.getClass().getSimpleName());
    }
}
