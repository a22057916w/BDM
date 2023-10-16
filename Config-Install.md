## PC Configuration
| Item | Detail |
| - | :---: |
| Kernel | Window 10 Experience |
| CPU | i5-13500 |
| Cores | 20 |
| Memory | 64G |

## Install
#### Download Java runtime
Install Java runtime (version 8 and above) <br>
Add JAVA_HOME to System variable in Windows <br>
Reboot and verify Java installation

#### Download Scala
Install Scala <br>
Reboot and verify Scala installation 

#### Download Spark distribution
Extract the Spark TGZ file into TAR format. Extract the TAR file into a folder. E.g., C:\Spark \
Add SPARK_HOME to System variable in Windows \
Add %SPARK_HOME%\bin to the System variable: Path in Windows 

#### Download Windows Utilities Executable (for the corresponding Spark distribution)
Copy the Windows Utilities Executable to the Spark folder. E.g., C:\spark\spark-3.5.0-bin-hadoop3\bin \
Additional steps to rectify winutils not found error in spark-shell \
Add HADOOP_HOME to System variable in Windows \
Add %HADOOP_HOME%\bin to the System variable: Path in Windows \
Reboot and verify Spark installation 

#### Download PySpark
Install pyspark \
Add PYSPARK_PYTHON(same as python path) to System variable \
Reboot and verify Pyspark installation
