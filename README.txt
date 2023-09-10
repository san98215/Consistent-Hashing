This is our Consistent Hashing-based Naming Service

Seth Nye, Davis Webster, David Korsunsky

In order to compile and run:

Start by going into the bnserver directory and run the following command: "javac -d bin src/bnserver.java"

Next, goin into the nmserver directory on another node and run the following command: "javac -d bin src/nmserver.java"

We recommend pinging the vcf node that has the server assigned to it and copying that IP address into nsconfig files before running any program

Now type on the bnserver side: "java -d bin bnserver 'bnconfig.txt' " 'where bnconfig.txt' is the startup file for bnservers

Go to the other vcf nodes for your client and enter : "java -d bin nmserver 'nsconfig#.txt' " where 'nsconfig#.txt' is the startup file for nmservers (the '#' indicates which config file is being used)

From here you should be able to execute all respective commands for each server


“ This project was done in its entirety by David Korsunsky, Davis Webster, and Seth Nye. We hereby
  state that we have not received unauthorized help of any form ”

