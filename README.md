# Broadcast Group Simulation

This project is part of the Broadcast Group Project which includes the following subprojects:
* [Broker Implementation](https://github.com/MoeweX/moquette): Extension of moquette that supports broadcast groups
* [Broadcast Group Simulation](https://github.com/MoeweX/broadcast-group-simulation): A simulation of the broadcast group formation process

Today, communication between IoT devices heavily relies on fog-based publish/subscribe (pub/sub) systems. Communicating via the cloud, however, results in a latency that is too high for many IoT applications. This project is about a fog-based pub/sub system that integrates edge resources to improve communication latency between end devices in proximity. To this end, geo-distributed broker instances organize themselves in dynamically sized broadcast groups that are connected via a scale-able fog broker.

If you use this software in a publication, please cite it as:

### Text
TODO

### BibTeX
```
TODO
```

A full list of our [publications](https://www.mcc.tu-berlin.de/menue/forschung/publikationen/parameter/en/) and [prototypes](https://www.mcc.tu-berlin.de/menue/forschung/prototypes/parameter/en/) is available on our group website.

Experiments related to the publication are based on [commit](https://github.com/MoeweX/broadcast-group-simulation/commit/80f7c30c745adc08e13665f6b1ee5f612cb2fa37). The jar was started with `java -jar -Xmx16G BroadcastGroupSimulation.jar -i worldcities.csv -l 10,20,30,40,50,60,70,80,90,100,150,200,250,300,350,400 -p sim -a 1200,2400,3600,4800,6000,7200,8400,9600,10800,12000`.

## Instructions

This repository contains code for the simulation of the broadcast group formation process.
The goal of this simulation is to better understand how the latency threshold affects the number of broadcast groups, as well as involved overheads.

Required input:
- Broker names, locations, and lcms (leadership capability measures)
- fixed, global latency threshold
- an educated guess for latency per km (see data package section below)

Calculated output:
- a valid set of broadcast groups

The output is written to the simulation-result directory.
The visualization directory contains various iPython notebooks that can be used to visualize the simulation results.

## Quickstart

Run the main method in the Main.kt (code directory) file to start the simulation with randomly generated input values.
As an alternative, you might also build a jar with `maven package` and run it without supplying any parameters (jar will be stored in the `out` directory).
If you want to see more details of the simulation process, update the log level in the `log4j2.xml` file.

If the simulation stops during "Broker Initialization", give java more memory, e.g., with `-Xmx16G`.

To use pre-defined data as input, supply required information as program data arguments, e.g.: `-p sim -i worldcities.csv -l 10,20 -a 1200,2400`.
Input data is expected to be in the same format as the worldcities dataset of [simplemaps](https://simplemaps.com/data/world-cities).
You may also start the jar with the -h option to learn more about available program arguments.

## Simulation process

A single simulation comprises multiple ticks.
During a "tick", every individual broker sequentially executes 7 tasks.
Before the next task is started, every broker waits until all other brokers have finished the current task,
and the next tick is only started when all brokers finished the tasks of the current tick.

The tasks are as follows; more information on individual tasks can be found by inspecting the respective functions in BrokerKt:
1. Prepare for next tick
2. Broadcast group leaders send merge requests to other leaders
3. Leaders process received merge requests and negotiate who becomes the leader of the new broadcast group
4. Sending leaders are notified about final negotiating result (who joins whom)
5. Leaders that join another broadcast group notify their members
6. Brokers that have to join another broadcast group prepare
7. Brokers that have to join another broadcast group notify their new leader

The simulation process ends, once the following two conditions are true:
- all members have a latency to their leader that is below the defined latency threshold
- all leaders have a latency to all other leaders that is above the defined latency threshold

## The "data" package (code directory)

The data package contains code to calculate an appropriate ms/km value for the simulation based on [iPlane](https://web.eecs.umich.edu/~harshavm/iplane/) measurements.

To calculate a latency per km value based on the iPlane data, run the main methods of the included files in the
 following order:
1. IPlaneLoader.kt
2. IPlaneIpToLocation.kt (requires a secret from [ipinfo.io](https://ipinfo.io/))
3. IPlaneLocationEnricher.kt

You may also customize the calculation by updating the **conf** objects in each file's main method.

## Run Simulation on AWS

Find below helpful commands to run the simulation on AWS (expects to be executed in a fish shell).
- Build Jar with `mvn package`
- Set var with `set URL xxxxx`
- Copy to AWS
```bash
scp -i "sim.pem" out/BroadcastGroupSimulation.jar ec2-user@$URL:/home/ec2-user/
scp -i "sim.pem" data/simulation_input/worldcities.csv  ec2-user@$URL:/home/ec2-user/
```
- Run
```bash
sudo yum install java-11-amazon-corretto
screen -S experiment
java -jar -Xmx16G BroadcastGroupSimulation.jar -i worldcities.csv -l 10,20,30,40,50,60,70,80,90,100,150,200,250,300,350,400 -p sim -a 1200,2400,3600,4800,6000,7200,8400,9600,10800,12000
```
- Collect results
```bash
scp -i "sim.pem" -r ec2-user@$URL:/home/ec2-user/simulation-result/ .
scp -i "sim.pem" -r ec2-user@$URL:/home/ec2-user/logfile.log/ simulation-result/sim-logfile.log
```
