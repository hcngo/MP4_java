import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Page Link Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);
        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Popular Links");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);
        jobB.setMapperClass(TopLinksMap.class);
        jobB.setReducerClass(TopLinksReduce.class);
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable zero = new IntWritable(0);
        private final static IntWritable one = new IntWritable(1);

        List<Integer> league = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");

            for (String leagueString : Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"))) {
                league.add(new Integer(leagueString));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //context.write(<IntWritable>, <IntWritable>); // pass this output to reducer
            String line = value.toString();
            String[] linkTokens = line.trim().split(":");
            String page = linkTokens[0];
            String[] links = linkTokens[1].split(" ");
            for (int i = 0; i < links.length; i++) {
                if (links[i].trim().length() == 0) {
                    continue;
                }
                Integer linkToPage = new Integer(links[i].trim());
                if (!league.contains(linkToPage)) {
                    continue;
                }
                IntWritable linkKey = new IntWritable();
                linkKey.set(linkToPage);
                context.write(linkKey, one);
            }

            if (league.contains(new Integer(page))) {
                IntWritable pageKey = new IntWritable();
                pageKey.set(new Integer(page));
                context.write(pageKey, zero);
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //context.write(<IntWritable>, <IntWritable>); // print as final output
            int sum = 0;
            for (IntWritable intWritable : values) {
                sum += intWritable.get();
            }
            IntWritable sumValue = new IntWritable();
            sumValue.set(sum);
            context.write(key, sumValue);
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        List<String> pageCounts = new ArrayList<String>();
    
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Pair<String, String> pageLinkCountPair = new Pair<>(key.toString(), value.toString());
            pageCounts.add(pageLinkCountPair.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //Cleanup operation starts after all mappers are finished
            //context.write(<NullWritable>, <IntArrayWritable>); // pass this output to reducer
            String[] pageCountsArray = new String[pageCounts.size()];
            pageCounts.toArray(pageCountsArray);

            TextArrayWritable textArrayWritable = new TextArrayWritable(pageCountsArray);
            NullWritable nullWritable = NullWritable.get();
            context.write(nullWritable, textArrayWritable);
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, TextArrayWritable, IntWritable, IntWritable> {
        List<Pair<Integer, Integer>> links = new ArrayList<>();
        Map<Integer, Integer> rankMap = new HashMap<>();

        List<Integer> league = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");

            for (String leagueString : Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"))) {
                league.add(new Integer(leagueString));
            }
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            //context.write(<Text>, <IntWritable>); // print as final output
            for (TextArrayWritable textArrayWritable : values) {
                String[] linkArray= textArrayWritable.toStrings();
                for (int i = 0; i < linkArray.length; i++) {
                    String[] thePair = linkArray[i].split(",");
                    Pair<Integer, Integer> intPair = new Pair<>(Integer.parseInt(thePair[1]), Integer.parseInt(thePair[0]));
                    links.add(intPair);

                }
            }
            Collections.sort(links);

            for (int i = 0; i < links.size(); i++) {
                if (i == 0 || links.get(i).first > links.get(i - 1).first) {
                    rankMap.put(links.get(i).second, i);
                } else {
                    rankMap.put(links.get(i).second, i - 1);
                }
            }

            Collections.sort(league, Collections.reverseOrder());

            for (Integer leagueMember : league) {
                if (!rankMap.containsKey(leagueMember)) {
                    continue;
                }
                IntWritable page = new IntWritable();
                IntWritable rank = new IntWritable();
                page.set(leagueMember);
                rank.set(rankMap.get(leagueMember));
                context.write(page, rank);
            }
        }

    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return first + "," + second;
    }
}
