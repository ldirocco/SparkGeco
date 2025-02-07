# SparkGeco

**SparkGeco** is a tool for distributed compressive genomics, implementing fundamental pattern matching primitives using Apache Spark. With the exponential growth of genomic data, efficient storage and analysis have become critical challenges. SparkGeco leverages compressive genomics and distributed computing to enable scalable and high-performance genomic data processing. 

## Usage

SparkGeco is released as a JAR file (`sparkgeco-1.0.0.jar`) and can be integrated into any Apache Spark pipeline written in Java. To use SparkGeco in your Java project, include the JAR in your classpath and import the necessary classes.


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

Currently, SparkGeco provides the following specialized classes, each implementing a different compression technique:

- **`BpeRDD`** – Byte Pair Encoding (BPE)  
- **`ChenRDD`** – Chen-Wang Compression  
- **`LzwRDD`** – Lempel-Ziv-Welch (LZW)  
- **`FmRDD`** – FM-Index Compression  

To switch to a different compression technique, simply replace `BpeRDD` with the corresponding class (`ChenRDD`, `LzwRDD`, or `FmRDD`).  

