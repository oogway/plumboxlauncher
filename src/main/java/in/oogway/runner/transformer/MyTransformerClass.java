package in.oogway.runner.transformer;

public class MyTransformerClass implements PBTransformer {
    @Override
    public void transform() {
        System.out.println("Transformation class obtained. " + this.getClass().getSimpleName());
    }
}
