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





