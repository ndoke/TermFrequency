Nachiket Doke
800878686
ndoke@uncc.edu

Steps for running the programs

$ hadoop fs -mkdir input
$ echo "Hadoop is an elephant" > file0
$ echo "Hadoop is as yellow as can be" > file1
$ echo "Oh what a yellow fellow is Hadoop" > file2
$ hadoop fs -put file* input

//The above comments will make input files and plcae them in an 'input' folder from where they can be picked up by the program.

$ mkdir -p build
$ javac -cp /opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/* \WordCount.java -d build -Xlint 

//The above command compiles the program.

$ jar -cvf wordcount.jar -C build/ .
$ hadoop jar wordcount.jar org.myorg.WordCount input output

//These commands create a *.jar file for the program and run the program by taking input from input folder and placing output in the output folder. The TFIDF program requires two output files.
//Please make sure that there is no folder of the name output already present in the system.

$ hadoop fs -cat output/*

//The final command helps us in seeing the ouput.
