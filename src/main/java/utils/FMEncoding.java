package utils;
import com.dynatrace.index4j.indices.FmIndex;
import com.dynatrace.index4j.indices.FmIndexBuilder;

import scala.Tuple2;


public class FMEncoding {

    public static FmIndex encode(String text) {

        FmIndex fmi = new FmIndexBuilder()
                .setSampleRate(32)
                .setEnableExtraction(true)
                .build(text);
        return fmi;
    }



}
