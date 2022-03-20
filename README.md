# RecStep

The repo contains the code of **RecStep** which was described in the PVLDB 2019 paper http://www.vldb.org/pvldb/vol12/p695-fan.pdf , a reasearch Datalog engine prototype built on top of a parallel single-node relational system **[Quickstep](http://www.vldb.org/pvldb/vol11/p663-patel.pdf)**. If you find RecStep useful in your research, please consider citing:

    @article{10.14778/3311880.3311886,
    author = {Fan, Zhiwei and Zhu, Jianqiao and Zhang, Zuyu and Albarghouthi, Aws and Koutris, Paraschos and Patel, Jignesh M.},
    title = {Scaling-up in-Memory Datalog Processing: Observations and Techniques},
    year = {2019},
    issue_date = {February 2019},
    publisher = {VLDB Endowment},
    volume = {12},
    number = {6},
    issn = {2150-8097},
    url = {https://doi.org/10.14778/3311880.3311886},
    doi = {10.14778/3311880.3311886},
    journal = {Proc. VLDB Endow.},
    month = feb,
    pages = {695–708},
    numpages = {14}
    }

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

**Note:** python version >= 3.8 is required.

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
python3 quickstep_shell.py --mode network --initialize &
```
or in a separate terminal window:
```bash
python3 quickstep_shell.py --mode network --initialize
```

4. Finally, you can start run RecStep to evaluate the datalog program 
``` bash
python3 interpreter.py <datalog_program_file_path> 
```
For this specific example, you should run the command:
``` bash
python3 interpreter.py ./benchmark_datalog_programs/tc.datalog
```

5. Check your results directly using the interactive quickstep shell:
One of the advantage of RecStep is that after running your program, you can look at your results and may perform further 
analysis using by running familiar SQL queries! To enter the quickstep shell in the interactive mode, run the command:
``` bash
python3 quickstep_shell.py --mode interactive
```
command "\d" could be used to list all the tables in the current quickstep database instance. The "qsstor" folder contains all the data files of the current database instance. More details regarding the use of quickstep can be found at **[Quickstep](https://github.com/apache/incubator-retired-quickstep)**. If you encounter other issues when using RecStep or the quickstep shell, you could send an email to zhiwei@cs.wisc.edu for more help and we will get back to you at our earliest convenience.

**Note:** Parallel Bit-Matrix Evaluation (PBME) has not been intergrated into the RecStep compiler yet. PBME has been designed/implemented specifically for the "dense graph" computation to prove its efficiency in cases where the size of input graph is relatively small in terms of number of vertices but the intermediate results are huge. We currently support PBME evaluation on *Transitive Closure (tc)* and *Same Generation (sg)* as stated in the paper but they are not directly runnable from the compiler itself.

