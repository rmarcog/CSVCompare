Developed with Hadoop 2.6.0
Tested with Cloudera VM 5.1.0

Parameters:
<xls file name 1> <xls file name 2> <column file1> <column file2> <output path>

xls file name 1/2 - The path to the files to compare. They must be in Hadoop Filesystem (HDFS).
column file 1/2 - The column to use as comparison key in file 1 and 2, starting by 1 (for the first column). A column is separated by ";" or ",".
output path - The output folder in HDFS. It must not exist.

The output is the lines of the file 1 which have a line in the file 2 with the same comparison key. At the end of the output, the percentage of matching lines is shown, together with the total of lines in the file.

----------------------------------------
How to run the program
----------------------------------------

1. Copy input files to HDFS. For example:

	a. Create a new folder in HDFS:
		hadoop fs -mkdir Input
	b. Copy input files to the new folder
		hadoop fs -put Sigma48_pep.csv
		hadoop fs -put Sigma48_pep2.csv
2. Run jar you can find in target folder:
	hadoop jar target/CSVCompare.csv Input/Sigma48_pep.csv Input/Sigma48_pep2.csv 1 1 Output
3. After job completion, check the output:
	hadoop fs -cat Output/part-r-00000

