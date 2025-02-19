package utils;

import fastdoop.FASTAlongInputFileFormat;
import fastdoop.PartialSequence;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

public class FmRDD extends JavaRDD<byte[]> {

    public FmRDD(RDD<byte[]> rdd) {
        super(rdd, ClassManifestFactory.fromClass(byte[].class));
    }

    public static FmRDD toFmRDD(JavaRDD<byte[]> fromRDD) {
        return new FmRDD(fromRDD.map(FmEncoding::encode).rdd());
    }

    public static FmRDD read(String path, JavaSparkContext sc, int partitions) {
        JavaRDD<byte[]> sequence = sc.newAPIHadoopFile(path, FASTAlongInputFileFormat.class,
                        Text.class, PartialSequence.class, sc.hadoopConfiguration()).
                map(t -> t._2.getValue().toUpperCase().replaceAll("[^ACGT]+", "").getBytes())
                .repartition(partitions);

        return toFmRDD(sequence);
    }
    public JavaRDD<Integer[]> search(String pattern) {

        return map(fm_index -> fm_index.count(pattern));
    }

    @Override
    public FmRDD cache() {
        return new FmRDD(super.cache().rdd());
    }

    @Override
    public FmRDD persist(StorageLevel level) {
        return new FmRDD(super.persist(level).rdd());
    }


}
