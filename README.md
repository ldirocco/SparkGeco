# SparkGeco

**SparkGeco** is a tool for distributed compressive genomics, implementing fundamental pattern matching primitives using Apache Spark. With the exponential growth of genomic data, efficient storage and analysis have become critical challenges. SparkGeco leverages compressive genomics and distributed computing to enable scalable and high-performance genomic data processing. 

## Usage

SparkGeco is released as a JAR file (`sparkgeco-1.0.0.jar`) and can be integrated into any Apache Spark pipeline written in Java. To use SparkGeco in your Java project, include the JAR in your classpath and import the necessary classes.

Currently, SparkGeco provides the following specialized classes, each implementing a different compression technique:

- **`BpeRDD`** – Byte Pair Encoding (BPE)  
- **`ChenRDD`** – Chen-Wang Compression  
- **`LzwRDD`** – Lempel-Ziv-Welch (LZW)  
- **`FmRDD`** – FM-Index Compression  

### Example

Below, we provide an example demonstrating how to compress genomic sequences and search for specific patterns using SparkGeco.  

In this example, we first set the path of the input sequences from the command-line arguments. The input file must be in **FASTA** or **FASTQ** format, or a directory containing multiple **FASTA** and **FASTQ** files. Then, a list of query patterns is defined. The Spark environment is initialized, and the sequences are read from a file to create a `BpeRDD` object. Finally, for each query, the code performs a search, aggregates the results, and prints the total number of occurrences found.  


  ```java
public class Main {
    public static void main(String[] args) {
        
        // Path of the sequences to compress
        String input_file = args[0]; 

        // Queries
        List<String> Ps = new ArrayList<>();
        Ps.add("TTCCTTAGGAAAAGGGGAAGACCACCAATC");
        Ps.add("AGAGGATTATGTACATCAGCACAGGATGCA");
        Ps.add("GAAGGACTTAGGGGAGTCCTCATGAAAAAT");
        Ps.add("GTATTAGTACAGTAGAGCCTTCACCGGCAT");
        Ps.add("TCTGTTTATTAAGTTATTTCTACAGCAAAA");
        Ps.add("CGATCATATGCAGATCCGCAGTGCGCGGTA");

        SparkConf conf = new SparkConf().setMaster("yarn");
        JavaSparkContext sc;

        BpeRDD sequence = BpeRDD.read(input_file, sc);

        long found = 0;
        
        for (String P : Ps)
                found += sequence
                        .search(P)
                        .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);

        System.out.println("Found: " + found);
    }
}


  ```


To switch to a different compression technique, simply replace `BpeRDD` with the corresponding class (`ChenRDD`, `LzwRDD`, or `FmRDD`).  

## Developing a Spark Application to run on a Distributed System

To develop a Java application that integrates Apache Spark and SparkGeco, follow these steps:  

### 1. Set Up Your Java Project  

Ensure you have the following installed:  
- Java 8 or later  
- Apache Spark (compatible version)  
- Maven or Gradle for dependency management  

#### **Maven Integration**

1. Download the `sparkgeco-1.0.0.jar` file and place it in a directory (e.g., `libs/`) inside your project.
2. Add the JAR to your Maven project by modifying the `pom.xml`:

```xml
<dependencies>
    <!-- Apache Spark dependencies -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId>
        <version>3.3.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.12</artifactId>
        <version>3.3.0</version>
    </dependency>

    <!-- SparkGeco JAR -->
    <dependency>
        <groupId>your.organization</groupId>
        <artifactId>sparkgeco</artifactId>
        <version>1.0.0</version>
        <scope>system</scope>
        <systemPath>${project.basedir}/libs/sparkgeco-1.0.0-all.jar</systemPath>
    </dependency>
</dependencies>
 ```

#### **Gradle Integration**

1. Download the sparkgeco-1.0.0-all.jar file and place it in a directory (e.g., libs/) inside your project.
2. Add the JAR to your Gradle project by modifying the build.gradle

```xml
dependencies {
    implementation 'org.apache.spark:spark-core_2.12:3.3.0'
    implementation 'org.apache.spark:spark-sql_2.12:3.3.0'

    // SparkGeco JAR
    implementation files('libs/sparkgeco-1.0.0-all.jar')
}
```

### 2. Write Your Java Application

See

### 3.  Package Your Application

Once your application is ready, package it into a JAR file using Maven:
```xml
mvn clean package
```

or Gradle:
```xml
gradle build
```

The JAR file will be generated inside the target/ or build/libs/ directory.

### 4. Running on a Spark Cluster
To run your Spark application on a cluster, use the spark-submit command:
```xml
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class your.package.SparkGecoApp \
  --jars sparkgeco-1.0.0-all.jar \
  your-application.jar /path/to/input
```

### 5. Deploying on Google Cloud Dataproc

Google Cloud Dataproc provides a managed Spark environment. Follow these steps to deploy your SparkGeco application:

#### 5.1 Create a Dataproc Cluster
```xml
gcloud dataproc clusters create sparkgeco-cluster \
    --region us-central1 \
    --single-node \
    --master-machine-type n1-standard-4 \
    --image-version 2.0-debian10
```

#### 5.2 Upload Your JAR to Google Cloud Storage
```xml
gsutil cp your-application.jar gs://your-bucket/
gsutil cp sparkgeco-1.0.0-all.jar gs://your-bucket/
```


### 6. Dataset

SparkGeco has been extensively tested by using genomic sequences from the following species available on NCBI:
1. [Picea Abies](https://www.ncbi.nlm.nih.gov/datasets/taxonomy/3329/)
2. [Picea Glauca](https://www.ncbi.nlm.nih.gov/datasets/taxonomy/3330/)
3. [Pinus Taeda](https://www.ncbi.nlm.nih.gov/datasets/taxonomy/3352/)
4. [SARS-CoV-2](https://www.ncbi.nlm.nih.gov/datasets/taxonomy/2697049/)


### 7.Reproducibility of Experiments  

To reproduce the experiments conducted with SparkGeco, we evaluated the execution time of pattern counting using all available data structures (`BpeRDD`, `ChenRDD`, `LzwRDD`, and `FmRDD`). We varied the number of computing units and the size of the input dataset to analyze scalability and performance. In this section, we provide a script for benchmarking and detailed instructions on how to run experiments on a Spark cluster.  

#### Experiment Script  

The following Java program evaluates the execution time for pattern counting on all RDD structures supported by SparkGeco. It takes as input:  
- The path to the input dataset (FASTA or FASTQ format)  
- The path to a text file containing query patterns (one pattern per line)  

The script then initializes each compression structure, performs the pattern search, measures the execution time, and prints the results. Each data structure is explicitly written without using loops.  

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.List;

public class BenchmarkSparkGeco {

    public static void main(String[] args) {
        
        // Check arguments
        if (args.length < 2) {
            System.out.println("Usage: BenchmarkSparkGeco <input_file> <query_file>");
            System.exit(1);
        }

        // Path to the input dataset
        String input_file = args[0]; 

        // Path to the file containing queries
        String query_file = args[1];
        List<String> Ps;
        try {
            Ps = Files.readAllLines(Paths.get(query_file));
        } catch (IOException e) {
            System.err.println("Error reading query file: " + e.getMessage());
            return;
        }

        SparkConf conf = new SparkConf().setAppName("SparkGeco Benchmark").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Benchmarking BpeRDD
        System.out.println("Benchmarking: BpeRDD");
        BpeRDD bpeSequence = BpeRDD.read(input_file, sc);

        long startTimeBpe = System.currentTimeMillis();
        long foundBpe = 0;
        for (String P : Ps)
            foundBpe += bpeSequence
                    .search(P)
                    .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);
        long endTimeBpe = System.currentTimeMillis();

        System.out.println("Found (BpeRDD): " + foundBpe);
        System.out.println("Execution Time (BpeRDD): " + (endTimeBpe - startTimeBpe) + " ms");

        // Benchmarking ChenRDD
        System.out.println("Benchmarking: ChenRDD");
        ChenRDD chenSequence = ChenRDD.read(input_file, sc);

        long startTimeChen = System.currentTimeMillis();
        long foundChen = 0;
        for (String P : Ps)
            foundChen += chenSequence
                    .search(P)
                    .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);
        long endTimeChen = System.currentTimeMillis();

        System.out.println("Found (ChenRDD): " + foundChen);
        System.out.println("Execution Time (ChenRDD): " + (endTimeChen - startTimeChen) + " ms");

        // Benchmarking LzwRDD
        System.out.println("Benchmarking: LzwRDD");
        LzwRDD lzwSequence = LzwRDD.read(input_file, sc);

        long startTimeLzw = System.currentTimeMillis();
        long foundLzw = 0;
        for (String P : Ps)
            foundLzw += lzwSequence
                    .search(P)
                    .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);
        long endTimeLzw = System.currentTimeMillis();

        System.out.println("Found (LzwRDD): " + foundLzw);
        System.out.println("Execution Time (LzwRDD): " + (endTimeLzw - startTimeLzw) + " ms");

        // Benchmarking FmRDD
        System.out.println("Benchmarking: FmRDD");
        FmRDD fmSequence = FmRDD.read(input_file, sc);

        long startTimeFm = System.currentTimeMillis();
        long foundFm = 0;
        for (String P : Ps)
            foundFm += fmSequence
                    .search(P)
                    .aggregate(0L, (v, arr) -> arr.length + v, Long::sum);
        long endTimeFm = System.currentTimeMillis();

        System.out.println("Found (FmRDD): " + foundFm);
        System.out.println("Execution Time (FmRDD): " + (endTimeFm - startTimeFm) + " ms");

        sc.close();
    }
}
```

#### Running the Experiment on a Spark Cluster
To run the experiment on a distributed Spark cluster, follow these steps:

1. Package the Application

Use Maven: mvn clean package
Or Gradle: gradle build

2. Upload the JAR to the Cluster
If using Google Cloud Dataproc:

```xml
gsutil cp your-application.jar gs://your-bucket/
gsutil cp sparkgeco-1.0.0-all.jar gs://your-bucket/
```

3. Submit the Spark Job
```xml
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class BenchmarkSparkGeco \
  --jars gs://your-bucket/sparkgeco-1.0.0-all.jar \
  --executor-memory 4G \
  --executor-cores 4 \
  --num-executors 8 \
  gs://your-bucket/your-application.jar /path/to/input /path/to/queries.txt
```
  
#### Scalability and Performance Experiments
To assess scalability and performance, repeat the experiments by varying:

1. Dataset Size: Use different input datasets stored in a distributed storage (e.g., HDFS).
2. Number of Computing Units: Adjust --num-executors, --executor-cores, and --executor-memory.
3. Compression Technique: The script evaluates all available RDD structures.





