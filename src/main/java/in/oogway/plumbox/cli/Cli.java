package in.oogway.plumbox.cli;

import in.oogway.plumbox.launcher.Ingester;
import in.oogway.plumbox.launcher.Pipeline;
import in.oogway.plumbox.launcher.Source;
import in.oogway.plumbox.launcher.View;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

public class Cli {
    private static HashMap<String, CliHandler> handlers = new HashMap<>();
    private static Option property = null;
    private static Options options = new Options();
    private static CommandLineParser bparser = new DefaultParser();

    static {
        Option opt = Option.builder("P").hasArgs()
                .build();

        options.addOption(opt);

        handlers.put("declare-source", (Plumbox pb, HashMap<String, String> ns) -> {
            pb.declare(new Source());
        });

        handlers.put("declare-ingester", (Plumbox pb, HashMap<String, String> ns) -> {
            String source = ns.get("uri");
            Source f = (Source) pb.get(source, Source.class);

            if (f == null) {
                throw new RuntimeException(String.format("%s not found", source));
            }

            String transformation = ns.get("pipeline");
            Pipeline t = (Pipeline) pb.get(transformation, Pipeline.class);
            if (t == null) {
                throw new RuntimeException(String.format("%s not found", transformation));
            }

            pb.declare(new Ingester(
                    ns.get("uri"),
                    ns.get("sink"),
                    ns.get("pipeline")));
        });

        handlers.put("declare-pipeline", (Plumbox pb, HashMap<String, String> ns) -> {
            pb.declare(new Pipeline(
                    ns.get("stages")));
        });

        ArrayList<Class> getters = new ArrayList<Class>();
        getters.add(Source.class);
        getters.add(Pipeline.class);
        getters.add(Ingester.class);
        getters.add(View.class);

        for (Class entity: getters) {
            addGetter(entity);
            addGetAll(entity);
        }
    }

    private static <T> void addGetter(Class<T> entity) {
        String handlerOne = String.format("get-%s", entity.getSimpleName().toLowerCase());
        handlers.put(handlerOne, (Plumbox pb, HashMap<String, String> ns) -> {
            System.out.println(pb.get(ns.get("id"), entity));
        });
    }

    private static <T> void addGetAll(Class<T> entity) {
        String handlerAll = String.format("get-%ss", entity.getSimpleName().toLowerCase());
        handlers.put(handlerAll, (Plumbox pb, HashMap<String, String> ns) -> {
            pb.getAll(entity.getSimpleName().toLowerCase(), entity)
                    .forEach((k, n) ->
                            System.out.println(String.format("%s -> %s", k, n)));
        });
    }

    public static void execute(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println(String.format("Must provide a subcommand"));
            System.exit(127);
        }

        String cmd = args[0];
        CliHandler handler = handlers.get(cmd);
        if (handler == null) {
            System.out.println(String.format("No matching handler for %s", cmd));
            System.exit(127);
        }

        CommandLine line = bparser.parse(options, args);
        HashMap<String, String> options = new HashMap<>();
        Properties ns = line.getOptionProperties("P");
        Enumeration<?> d = ns.propertyNames();

        while (d.hasMoreElements()) {
            Object val = d.nextElement();
            options.put((String) val, (String) ns.get(val));
        }

        String host = System.getProperty("REDIS_HOST");
        if (StringUtils.isEmpty(host)) {
            host = "localhost";
        }

        Plumbox pb = new Plumbox(new RedisStorage(host));
        handler.run(pb, options);
    }
}
