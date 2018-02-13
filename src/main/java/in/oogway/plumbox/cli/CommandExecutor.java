package in.oogway.plumbox.cli;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class CommandExecutor {

    private ArgumentParser parser = ArgumentParsers.newFor("plumbox").build();
    private Subparsers subparsers = parser.addSubparsers().dest("cmd");
    private Namespace res;

    public CommandExecutor() throws ArgumentParserException {

        Subparser parserInit = subparsers.addParser("init");
        parserInit.addArgument("--filesystem").type(String.class).required(true).dest("filesystem").choices("LOCAL", "HDFS", "REDIS");
        parserInit.addArgument("--path").type(String.class).required(true).dest("path");

        Subparser declareFact = subparsers.addParser("declare-fact");
        declareFact.addArgument("--source").type(String.class).required(true).dest("source");
        declareFact.addArgument("--sink").type(String.class).required(true).dest("sink");
        declareFact.addArgument("--driver").type(String.class).required(true).dest("driver");
        declareFact.addArgument("--input_table").type(String.class).required(true).dest("input_table");
        declareFact.addArgument("--output_table").type(String.class).required(true).dest("output_table");

        Subparser declareDimension = subparsers.addParser("declare-dimension");
        declareDimension.addArgument("--source").type(String.class).required(true).dest("source");
        declareDimension.addArgument("--sink").type(String.class).required(true).dest("sink");
        declareDimension.addArgument("--driver").type(String.class).required(true).dest("driver");
        declareDimension.addArgument("--input_table").type(String.class).required(true).dest("input_table");
        declareDimension.addArgument("--output_table").type(String.class).required(true).dest("output_table");

        Subparser declareView = subparsers.addParser("declare-view");
        declareView.addArgument("--source").type(String.class).required(true).dest("source");
        declareView.addArgument("--sink").type(String.class).required(true).dest("sink");
        declareView.addArgument("--query").type(String.class).required(true).dest("query");

        Subparser declareTransformation = subparsers.addParser("declare-transformation");
        declareTransformation.addArgument("--sink").type(String.class).required(true).dest("sink");
        declareTransformation.addArgument("--classes").nargs("+").type(String.class).required(true).dest("classes");

        Subparser declareIngester = subparsers.addParser("declare-ingester");
        declareIngester.addArgument("--source").type(String.class).required(true).dest("source");
        declareIngester.addArgument("--sink").type(String.class).required(true).dest("sink");
        declareIngester.addArgument("--transformation").type(String.class).required(true).dest("transformation");

        Subparser listFacts = subparsers.addParser("list-facts");
        Subparser listDimensions = subparsers.addParser("list-dimensions");
        Subparser listViews = subparsers.addParser("list-views");
        Subparser listTransforms = subparsers.addParser("list-transformations");
        Subparser listIngesters = subparsers.addParser("list-ingesters");

    }

    public void init () throws IOException {
        Plumbox pb = Plumbox.createPlumbox();
        pb.initialize(this.res.get("filesystem"), this.res.get("path"));
        pb.readConfiguration();
    }

    private Plumbox getPlumboxObj() {

        Plumbox pb = Plumbox.createPlumbox();

        pb.setPbData("source", (String)this.res.get("source"))
                .setPbData("sink", (String)this.res.get("sink"))
                .setPbData("driver", (String)this.res.get("driver"))
                .setPbData("input_table", (String)this.res.get("input_table"))
                .setPbData("output_table", (String)this.res.get("output_table"))
                .setPbData("query", (String)this.res.get("query"))
                .setPbData("transformation", (String)this.res.get("transformation"))
                .setPbData("classes", (List<String>)this.res.get("classes"));

        return pb;
    }

    public void declare_fact() {
        Plumbox pb = this.getPlumboxObj();
        pb.declareFact();
    }

    public void declare_dimension() {
        Plumbox pb = this.getPlumboxObj();
        pb.declareDimension();
    }

    public void declare_view() {
        Plumbox pb = this.getPlumboxObj();
        pb.declareView();
    }

    public void declare_transformation() {
        Plumbox pb = this.getPlumboxObj();
        pb.declareTransformation();
    }

    public void declare_ingester() {
        Plumbox pb = this.getPlumboxObj();
        pb.declareIngester();
    }

    public void list_facts() {
        Plumbox pb = Plumbox.createPlumbox();
        pb.listFacts();
    }

    public void list_dimensions() {
        Plumbox pb = Plumbox.createPlumbox();
        pb.listDimensions();
    }

    public void list_views() {
        Plumbox pb = Plumbox.createPlumbox();
        pb.listViews();
    }

    public void list_transformations() {
        Plumbox pb = Plumbox.createPlumbox();
        pb.listTransformations();
    }

    public void list_ingesters() {
        Plumbox pb = Plumbox.createPlumbox();
        pb.listIngesters();
    }

    public void execute(String[] args) {
        try {

            this.res = parser.parseArgs(args);
            String cmd = res.get("cmd");
            String methodName = cmd.replaceAll("-","_");

            Method method = this.getClass().getMethod(methodName);
            method.invoke(this);

        } catch (ArgumentParserException e) {
            parser.handleError(e);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
