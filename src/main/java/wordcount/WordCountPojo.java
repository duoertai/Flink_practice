package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import wordcount.util.WordCountData;

public class WordCountPojo {
    public static class Word {
        private String word;
        private int frequency;

        public Word() {

        }

        public Word(String word, int i) {
            this.word  = word;
            this.frequency = i;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        public String toString(){
            return "Word = " + word + " freq = " + frequency;
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = WordCountData.getDefaultTextLineDataSet(env);
        }

        DataSet<Word> counts = text.flatMap(new Tokenizer()).groupBy("word").reduce(new ReduceFunction<Word>() {
            public Word reduce(Word value1, Word value2) throws Exception {
                return new Word(value1.word, value1.frequency + value2.frequency);
            }
        });

        if (params.has("output")) {
            counts.writeAsText(params.get("output"), WriteMode.OVERWRITE);
            env.execute("Word Count Pojo Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path");
            counts.print();
        }
    }

    public static final class Tokenizer implements FlatMapFunction<String, Word> {

        public void flatMap(String value, Collector<Word> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String token: tokens) {
                if (token.length() > 0) {
                    out.collect(new Word(token, 1));
                }
            }
        }
    }
}
