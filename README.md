# Publius: Building Serverless Cost-Optimized Distributed Systems (SCODS)
Publius is a framework for developing SCODS. A SCODS has a following characteristics:
* It is pay-as-you-go, and cost-optimal under low and high usage.
* Its performance is state-of-the-art under high usage and acceptable under low usage.
* A SCODS should seamless switch between the two cloud-native models (FaaS+BaaS and container-based).
* Switching between models is transparent to the users, there is no management of hardware.
* A SCODS is deployable to AWS without any custom infrastructure.

## Why Publius?
* Three Aims of Serverless Computing:
   * Fine-grained pay-as-you-go billing model.
   * Unlimited scalability.
   * No management of hardware or of VMs.
* **Allocation/Usage Problem**: Serverless Computing is enabled by **separation of resource usage from resource allocation**.
   * Cloud vendor may have a pool of constantly running nodes.
   * But only charges users upon usage in a fine-grained manner.
   * Necessary to avoid slow node startups upon usage.
   * **To recoup costs, vendors charge a premium for usage. Serverless computing is expansive under high usage.**
* Serverless Programming = FaaS + BaaS:
   * Functions as a Service (e.g. Lambda) provide general programmability.
      * With limited storage and communication.
   * Backends as a Service (e.g. S3) provides systems that augments FaaS.
      * Generally storage  or communication heavy systems such as DBMSs or File Systems.
   * Serverless Programming can only be provided by a cloud vendor:
      * Must have preexisting capacity to solve the allocation/usage problem.
* **Serverless Development Problem**:
   * **A wide range of systems cannot be built by researchers and open-source developers.**
   * **For example, all existing serverless key-value stores or DBMSs are commercial.**
* Previous research has attempted to solve the serverless development problem.
   * They inevitably reintroduce the allocation/usage problem by attempting to replace AWS Lambda.
   * Their main downside is that they require infrastructure changes by a cloud provider.
* Publius addresses both the allocation/usage problem and the development problem.
* It combines the two cloud-native programming models:
   * The FaaS+BaaS model:
      * Cost-optimal under low utilization (fine-grained pay-as-you-go; scales to zero).
      * Has higher, but acceptable latencies under flexible SLAs.
   * Container-based model:
      * Cost-optimal under high utilization.
      * State-of-the-art latencies and throughput.
* By optimizing for cost, we can combine the two models and get the best of both worlds.


## Usage
### Setting Up
TODO: Talk about Rust, Docker and AWS credentials.

### Basic Usage
TODO: Talk about functions, actors and WALs.

### Extensibility
TODO: Talk about extending Publius.

### Benchmarking
TODO: Talk about benchmarking Publius.

## Examples
We built:
1. Serverless SBTree. (TODO: Show source code.)
2. Machine Learning Prediction server. (TODO: Show source code.)
