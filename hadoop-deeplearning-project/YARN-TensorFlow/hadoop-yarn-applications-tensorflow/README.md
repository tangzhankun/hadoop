TensorFlow on YARN
======================
TensorFlow on YARN is a YARN application to enable an easy way for end user to run TensorFlow scripts.

Note that current project is a prototype with limitation and is still under development

## Features
- [x] Launch a TensorFlow cluster with specified number of worker and PS server
- [x] Replace python layer with java bridge layer to start server
- [x] Generate ClusterSpec dynamically
- [x] RPC support for client to get ClusterSpec from AM
- [x] Signal handling for graceful shutdown
- [x] Package TensorFlow runtime as a resource that can be distributed easily
- [ ] TensorBoard support
- [ ] Better handling of network port conflicts
- [ ] Fault tolerance
- [ ] Code refine and more tests

## Quick Start Guide 
### Set up
1. Git clone ..
2. Compile [tensorflow-bridge](../tensorflow-bridge/README.md) and put "libbridge.so", "libgrpc_tensorflow_server.so" to "bin" directory that yarn-tf belongs.
3. Compile TensorFlow on YARN

   ```sh
   cd <path_to_hadoop-yarn-application-tensorflow>
   mvn clean package -DskipTests
   ```

### Modify Your Tensorflow Script

1. Since we'll launch TensorFlow servers first and then run your script, the codes that start and join servers is no more needed:
   
    ```
    // the part of your script like the following need to be deleted                       
    server = tf.train.Server(clusterSpec, job_name="worker", task_index=0)      
    server.join()                   
    ```

2. Parse ps and worker servers
   
   Note that at present, you should parse worker and PS server from parameters "ps" and "wk" populated by TensorFlow on YARN client in the form of comma seperated values.
   
3. For a detailed between-graph MNIST example, please refer to:
   [job.py](samples/between-graph/job.py)
   [mnist-client.py](samples/between-graph/mnist-client.py)


### Run  
Run your Tensorflow script. Let's assume a "job.py"

   ```sh
   cd bin
   yarn-tf -job job.py -numberworkers 4 -numberps 1 -jar <path_to_tensorflow-on-yarn-with-dependency_jar>
   ```
