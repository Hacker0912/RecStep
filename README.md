# RecStep

The repo contains the code of **[RecStep](http://www.vldb.org/pvldb/vol12/p695-fan.pdf)**, a reasearch Datalog engine prototype built on top of a parallel single-node relational system **[Quickstep](http://www.vldb.org/pvldb/vol11/p663-patel.pdf)**. 

**Note:** The following set-up steps have been specifically tested on *Ubuntu 18.04.1 LTS*. And thus we recommend using *Ubuntu 18.04.1 LTS* as your testbed OS if you want to play with RecStep. It should also be feasible to set-up the RecStep backend on Ubuntu of other versions (e.g., 14.04, 16.04), but it may require a little bit more efforts as configuring the corresponding dependencies as required in Ubuntu of different versions might be slighly different. 


### Set-up Instructions

**1. Before getting started, let's first set-up the relational backend *Quickstep***

The instruction can be found at https://github.com/Hacker0912/quickstep-datalog

**2. Check/Resolve potential dependency issues:**
 
``` bash
sudo apt-get install -y python3-pip python-dev build-essential
sudo pip3 install psutil
sudo pip3 install antlr4-python3-runtime
```
**3. Checkout the code:**
```bash
git clone https://github.com/Hacker0912/RecStep/
```

* We need to change the value of "Quickstep_Shell_Dir" in "Config.json" to be the directory containing the compiled Quickstep executable binaries (***quickstep_client*** and ***quickstep_cli_shell***). For example, if ***quickstep-datalog*** repo is cloned into the path ***/fastdisk/local/quickstep-datalog***, then you should set the value to be ***"/fastdisk/local/quickstep-datalog/build"***

* You can also change the values of other configuraiton variables in "Config.json" for your purposes, such as the input data directory ***Input_Dir*** and the number of threads you want to use when running RecStep ***threads_num***. But I recommend you to keep other configuration values
as they are for easiness. Also, the ***Logging***, ***Debug*** configuration variables were only there for debugging/analysis purposes just as their names suggest - so you can just leave them unchanged. ***Optimization*** configuration variables are already set in the *optimal* way as stated in the paper, but we may temporarily need to change them a little bit for now if we want to run datalog programs involving *recursive-aggregation* (e.g., benchmark_programs/cc.datalog, benchmark_programs/sssp.datalog) - set ***dynamic_set_diff*** to be false. This is mainly due to the emerging bugs recently found in the backend due to the third-party dependency updates - we will get it fixed later and then this limit will be gone. 

That's it! And now you can start playing with *RecStep*, let's get into more details with a toy example: 

1. You should first define the datalog program including IDB/EDB relations along with the datalog rules. Example datalog program files are provided in the folder ***benchmark_datalog_programs***, the suffix of which is ".datalog". The syntax is very simple and it should be easy to follow what are presented in these examples and write your own datalog program. As a research prototype, we note that we mainly focus relations in which all data types are *int*. Though RecStep techniqcally supports other data types that are supported by **[Quickstep](https://github.com/apache/incubator-retired-quickstep)** as well, we have not fully teseted these types and we hope users are aware of this fact when they are trying RecStep. We will use ***tc.datalog*** here as an example due to its simplicity. 


2. Now you should put the input files (which should be in csv format as required) into the input directory as configured earlier (which by default is "./Input"). For example, in *tc.datalog*, the 
only input/EDB is **arc(x  int, y int)**, a relation with two integer attributes. Then the input file should have ***exactly the same name*** as the relation plus the ***.csv*** after. Each line of the input file represents a single tuple and attributes are seperated by ','. An example file could also be found in the folder ***Input***. 

3. Then you should start the quickstep backend in the background:
```bash
python3 quickstep_shell.py --mode network &
```
or in a separate terminal window:
```bash
python3 quickstep_shell.py --mode network
```

4. Finally, you can start run RecStep to evaluate the datalog program 
``` bash
python3 interpreter.py <datalog_program_file_path> 
```
For this specific example, you should run the command:
``` bash
python3 interpreter.py ./benchmark_datalog_programs/tc.datalog
```

**Note:** Parallel Bit-Matrix Evaluation (PBME) has not been intergrated into the RecStep compiler yet. PBME has been designed/implemented specifically for the "dense graph" computation to prove its efficiency on the cases in which the size of input graph is relatively small in terms of number of vertices but the intermediate results are huge. We currently support PBME on *Transitive Closure (tc)* and *Same Generation (sg)* as stated in the paper, the evaluation of which will be available soon. 
