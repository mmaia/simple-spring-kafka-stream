This file contains step by step instructions on how to run this playground project.


## Pre requisites

You need to install the following before running this playground project:

- Java 11
- Maven
- Docker

To run the local kafka setup using docker compose: 

- Docker compose

Or if you prefer to run local setup using kubernetes: 

- Kind
- Kubectl

# Run it - Local infra

You can run the local setup using kubernetes OR docker compose.

### With kubernetes

If you need a bit more context on how to run the local setup using kubernetes, you can read this [Kubernetes post](https://dev.to/thegroo/running-kafka-on-kubernetes-for-local-development-with-storage-class-4oa9)

1. Open a terminal and cd to the `kubernetes` folder.
2. Inside `kubernetes` folder create a folder called "tmp" `mkdir tmp`, this is where the storage will be automatically 
   provisioned by the default Kind storage class.
3. Run kind specifying configuration: `kind create cluster --config=kind-config.yml`. This will start a kubernetes
   control plane + worker
4. Run kubernetes configuration for kafka `kubectl apply -f kafka-k8s`
5. When done stop kubernetes objects: `kubectl delete -f kafka-k8s` and then if you want also stop the kind cluster
   which:
   will also delete the storage on the host machine: `kind delete cluster`

### With docker compose

1. Run `docker-compose up -d` in the root folder of the project.

# Run it - The application

1. Build the application: `mvn clean package`
2. Run it: `mvn spring-boot:run`

## Calling endpoints

Some sample calls are provided in the `test-data.http` file. You can run those from IntelliJ directly or if you're using
VSCode you can run them after installing the `http` extension (REST Client). You can also convert those simple callst to
curl or httpie commands if that's your preference.

If you want to send multiple messages I recommend that you also check [Apache ab tool](https://httpd.apache.org/docs/2.4/programs/ab.html). 
