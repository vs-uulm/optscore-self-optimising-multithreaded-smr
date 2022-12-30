# OptSCORE -- Self-Optimising Application-Agnostic Multithreaded SMR

This repository contains the source code used for obtaining and demonstrating the results presented in the 2020 SRDS paper [Self-optimising Application-agnostic Multithreading for Replicated State Machines](https://doi.org/10.1109/SRDS51746.2020.00024).

The code is based on BFT-SMaRt, and all evaluations were performed on its version v1.2. Therefore, this version is included in this code base. Basic instructions on how to set up and run BFT-SMaRt clusters can be found in [README-BFT-SMaRt](README-BFT-SMaRt.md).


### Setup and Explanation of Benchmarking Environment

In order to run distributed benchmarks, which includes setting up multiple replicas, setting up clients, specifying test case parameters, gathering log files, and much more, a small benchmarking-framework was written and used to generate output for the paper mentioned above. This framework facilitates the creation, management, and running of benchmark runs (also called testcases).
Its architecture and usage instructions will be briefly described here. This information should suffice to reproduce our paper's findings. If there are any problems or questions, please contact any of the [persons of the OptSCORE research group at Ulm or Passau University](https://www.uni-ulm.de/in/in/vs/res/proj/optscore/), preferably [Gerhard Habiger](https://github.com/ghabiger).


#### The test-case.db database

To have a central repository of all testcases that were run, including their creation date, testcase parameters, number of runs, etc., a simple SQLite database with the necessary tables can be found in [test-case.db](./test-case.db).
To create new testcases, a small CLI-tool is provided with the class ```de.optscore.vscale.util.cli.TestcaseCreator```. Simply start this CLI tool and you will be guided through the creation process of a testcase.

A testcase consists of metadata (e.g. a name, creation date, number of finished runs), the machines used for running the testcase (specified by their IP-addresses), meaning both replica and client machines, and a workload, which is a set of playbook actions that are performed at certain times during the testcase. A playbook action in the context of this distributed benchmarking framework is either the activation or deactivation of clients from the pre-instantiated pool of clients required for this testcase.
Each client that can be activated is of a certain type, where a type specifies the requests this client will be sending to the replica group. Requests contain a series of actions, such as locking or unlocking a mutex or simulating a calculation to load a CPU core, etc.

A fully specified testcase which is started (more on this later) will

* Retrieve all of its details from the test-case.db,
* Determine all the required client and replica machines to run the testcase,
* Prepare these machines by automatically connecting to them using SSH and starting local instances of our BFT-SMaRt-based evaluation framework,
* Scan through all playbook actions to determine the number and type of required clients,
* Pre-allocate all of these client threads in a deactivated state to work around some lingering performance-busting funkyness when establishing fresh connections to BFT-SMaRt clusters during runtime,
* Synchronize all machines and start the evaluation run, meaning that all playbook actions are performed at their specified times,
* Shut down all created replicas and clients, and finally
* Gather all created logs (1 per machine) to the central host coordinating the testcase.

#### ClientMachines, ClientGroups and Workloads

First, the database has to be populated with at least one _ClientMachine_. ClientMachines are simple to specify: One only needs to enter their IP and port at which they will be reachable via SSH, alongside a short description if desired.

Then, at least one _ClientGroup_ is required, which is a logical grouping of several client threads sending the same type of requests at the same rate to the replica cluster. Therefore, to specify a ClientGroup, one needs to specify how many clients (= sending threads) this group should represent, which type of request (= sequence of actions) all of these clients should send, and how much delay time each client should wait inbetween requests (between receiving the final quorum-answer from the replica cluster and sending out a new request). The RequestProfile referenced here is specified in the codebase, see ```de.optscore.vscale.RequestProfile```.

Next, a _Workload_ can be created, which specifies the exact sequence of client (de-)activations during a testcase. For example, a simple workload starting at time 0ns could start a single client at offset 0ns (so right at the beginning), then add a second client at offset 5000000000ns (so after 5 seconds), then deactivate both those clients at 10000000000ns (so after 10 seconds).
This workload would have a _WorkloadPlaybook_ with 4 actions: The first at 0ns, activating one ClientGroup, then a second action at 5 seconds, activating another ClientGroup, then 2 actions at 10 seconds, deactivating both those activated groups.
Workloads can be re-used in many different testcases, e.g., to test the results of the same load under different parameters (as demonstrated in the paper).

#### Configurations and Testcases

A configuration is an entry in the pertaining table, with a 1:n relationship to testcases, and referencing a workload. This means that multiple testcases can reference the same configuration, which in turn references a workload.
Creating a testcase therefore requires a configuration, which can also created via the CLI tool. The configuration saves the main parameters of the testcase, e.g., whether UDS should be on or off (i.e., multithreading be active or not), the UDS configuration (primaries and steps per round), etc.
It also contains the IPs and ports of the replicas, which the testcase coordinator machine (see next sentence) will use in order to SSH into these machines and start BFT-SMaRt and coordinate the entire testcase suite.


Once a testcase has been fully specified, it can be started using ```de.optscore.vscale.coordination.TestcaseCoordinator```. Starting the coordinator with the test-case DB file path, hostname and port of the machine coordinating the testcase, and one or several testcase ID(s) will automatically run the specified testcases, if they can be found in the given database.
All logfiles created by the machines during the testcase will be automatically gathered on the coordinator host. Paths unfortunately have to be specified in source code.


### The pre-setup of replicas and client machines

In order for this to work, initially the machines (hardware) for running these benchmarks have to be set up. Distribute a build of the source code onto each replica and client machine, create a user with admin-rights which can be used by the testcase coordinator machine to start instances via SSH, configure the hosts.config on all hosts with the IPs of these machines, and enter the IPs of the client machines and replicas into the test-case.db.



### Manually starting replicas and clients

This is also possible. Commands can be found in the TestcaseCoordinator code, starting from line 480ff.