************************
*  PAGE RANK ALGORITHM *
************************

INSTRUCTION
-----------
1. Download zip file and extract it.
2. Copy your input files into ‘./input/’ directory which you will find 
   inside extracted folder.
3. Set r+w+x permissions for ‘run.sh’ (i.e. chmod 777 run.sh).
4. Execute ‘run.sh’ file. (Note: You may have to execute doc2unix command 
   on shell script file before executing it.)

Note: You can change any parameter set in shell script file and accordingly 
      you will get the results.


ADDITIONAL FEATURE
------------------
This application also handles sink nodes. Set HandleSinkNode=true for enabling sink node handling.
e.g.
    hadoop jar PageRank.jar org.myorg.pagerank.WikiPageRank "10" "0.85" "100" input/PageRank output/MapReduce2/PageRank/TopWikiPage true


MAINTAINER
----------
Name        Chetan Borse
EMail ID    cborse@uncc.edu
LinkedIn    https://www.linkedin.com/in/chetanrborse

