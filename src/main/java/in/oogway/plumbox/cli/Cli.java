package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.Pipeline;
import in.oogway.plumbox.launcher.Source;
import in.oogway.plumbox.launcher.View;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;

public class Cli {
    private static ArgumentParser parser = ArgumentParsers.newFor("plumbox").build();
    private static Subparsers subparsers = parser.addSubparsers().dest("cmd");
    private static HashMap<String, CliHandler> handlers = new HashMap<>();

    static {
        Subparser get = subparsers.addParser("sync");
        get.addArgument("--id").type(String.class).required(true).dest("id");
        handlers.put("sync", (Plumbox pb, Namespace ns) -> {
            Ingester i = (Ingester) pb.get(ns.get("id"), Ingester.class);
            SparkConfig sc = new SparkConfig("Plumbox Launcher");
            i.execute(pb.getDriver(), sc.getSession());
        });

        handlers.put("declare-source", (Plumbox pb, Namespace ns) -> {
            pb.declare(new Source(
                    ns.get("path"),
                    ns.get("driver"),
                    ns.get("uri")));
        });

        handlers.put("declare-ingester", (Plumbox pb, Namespace ns) -> {
            String source = ns.get("uri");
            Source f = (Source) pb.get(source, Source.class);

            if (f == null) {
                throw new RuntimeException(String.format("%s not found", source));
            }

            String transformation = ns.get("transformation");
            Pipeline t = (Pipeline) pb.get(transformation, Pipeline.class);
            if (t == null) {
                throw new RuntimeException(String.format("%s not found", transformation));
            }

            pb.declare(new Ingester(
                    ns.get("uri"),
                    ns.get("sink"),
                    ns.get("transformation")));
        });

        handlers.put("declare-transformation", (Plumbox pb, Namespace ns) -> {
            pb.declare(new Pipeline(
                    ns.get("stages")));
        });

        ArrayList<Class> getters = new ArrayList<Class>();
        getters.add(Source.class);
        getters.add(Pipeline.class);
        getters.add(Ingester.class);
        getters.add(View.class);

        for (Class entity: getters) {
            addDeclation(entity);
            addGetter(entity);
            addGetAll(entity);
        }
    }

    private static <T> void addDeclation(Class<T> entity) {
        String handlerDeclare = String.format("declare-%s", entity.getSimpleName().toLowerCase());
        Subparser declaration = subparsers.addParser(handlerDeclare);
        for (Field f : entity.getFields()) {
            String field_name = f.getName().toLowerCase();
            declaration.addArgument("--".concat(field_name))
                    .type(f.getType())
                    .required(true)
                    .dest(field_name);
        }
    }

    private static <T> void addGetter(Class<T> entity) {
        String handlerOne = String.format("get-%s", entity.getSimpleName().toLowerCase());
        Subparser get = subparsers.addParser(handlerOne);
        get.addArgument("--id").type(String.class).required(true).dest("id");
        handlers.put(handlerOne, (Plumbox pb, Namespace ns) -> {
            System.out.println(pb.get(ns.get("id"), entity));
        });
    }

    private static <T> void addGetAll(Class<T> entity) {
        String handlerAll = String.format("get-%ss", entity.getSimpleName().toLowerCase());
        subparsers.addParser(handlerAll);
        handlers.put(handlerAll, (Plumbox pb, Namespace ns) -> {
            pb.getAll(entity.getSimpleName().toLowerCase(), entity)
                    .forEach((k, n) ->
                            System.out.println(String.format("%s -> %s", k, n)));
        });
    }

    public static void execute(String[] args) throws ArgumentParserException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        Namespace ns = parser.parseArgs(args);
        String cmd = ns.get("cmd");

        String host = System.getProperty("REDIS_HOST");
        if (StringUtils.isEmpty(host)) {
            host = "localhost";
        }

        Plumbox pb = new Plumbox(new RedisStorage(host));

        handlers.get(cmd).run(pb, ns);
    }
}
