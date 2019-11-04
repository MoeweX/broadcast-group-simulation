# Broadcast Group Simulation

This repository contains code for the simulation of the broadcast group formation process.
The goal of this simulation is to better understand how the latency threshold affects the number of broadcast groups.

Required input:
- Broker names, locations, and lcms (leadership capability measures)
- fixed, global latency threshold
- an educated guess for latency per km

Calculated output:
- a valid set of broadcast groups

## Quickstart

Run the main method in the Main.kt file to start the simulation with randomly generated input values.
If you want to see more details of the simulation process, update the log level in the `log4j2.xml` file.

If it stops during "Broker Initialization", give java more memory: `-Xmx32G`

To use pre-defined data as input, supply required information as program data arguments (TBD).

## Simulation process

A single simulation comprises multiple ticks.
During a "tick", every individual broker sequentially executes 7 tasks.
Before the next task is started, every broker waits until all other brokers have finished the current task,
and the next tick is only started when all brokers finished the tasks of the current tick.

The tasks are as follows; more information on individual tasks can be found by inspecting the respective functions in
 BrokerKt:
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

## The "data" package

The data package contains code to create a pre-defined input data set based on [iPlane](https://web.eecs.umich.edu/~harshavm/iplane/) measurements.

To calculate a latency per km value based on the iPlane data, run the main methods of the included files in the
 following order:
1. IPlaneLoader.kt
2. IPlaneIpToLocation.kt (requires a secret from [ipinfo.io](https://ipinfo.io/))
3. IPlaneLocationEnricher.kt

You may also customize data set generation by updating the **conf** objects in each file's main method.

## Run on AWS

- Build Jar with `mvn package`
- Copy to AWS
```bash
scp -i "sim.pem" out/BroadcastGroupSimulation.jar ec2-user@URL:/home/ec2-user/
scp -i "sim.pem" data/simulation_input/worldcities.csv  ec2-user@URL:/home/ec2-user/
```
- Run and collect results
```bash
java -jar -Xmx16G BroadcastGroupSimulation.jar -i worldcities.csv -l 10,20,30,40,50,60,70,80,90,100,150,200,250,300,350,400,450,500 -p sim
scp -i "sim.pem" -r ec2-user@URL:/home/ec2-user/simulation-result/ .
scp -i "sim.pem" -r ec2-user@URL:/home/ec2-user/logfile.log/ sim-logfile.log

```
