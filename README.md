# Overview
This sample application shows reading change feed from a graph created in a Cosmos Gremlin API account. This sample uses the change feed processor from the Java SDK. The change feed processor in the current from can only be used by SQL accounts. So, one would wonder how it would work with Gremlin Account. Thanks to the multi-model capability of the Gremlin account, in addition to the gremlin endpoint, Cosmos Gremlin account also provides a document/SQL endpoint. In the azure portal you will see this labelled as ".NET SDK URI". We can use this document/SQL end point to start reading the change feed from the gremlin graph. 

What about the lease container? can it be created in the Gremlin API account? No, this is because Gremlin API account do not allow containers with "id" as the partition key. Which is a requirement for the lease collection. So, a SQL API account will have to be created to house the lease container. 

When you read the change feed you will see JSON document which will look like a SQL API document. Reason, you are using the document/SQL end point.

Each Vertex and Edge in the graph is stored as individual JSON document with a specific format. Every time a Vertex or an Edge is inserted or updated the corresponding JSON document shows up in the change feed. 


# Instructions

## First:
 * Maven
 * Create Gremlin API and SQL API accounts
 * Clone the repository

## Then:
* Update application.properties file with the credentials for both the Gremlin API and SQL API accounts 
* Run the application with mvn spring-boot:run 
* A Graph and a Lease container will be created in the corrosponding accounts
* Add a Vertex from azure portal using the following gremlin query, you will see the correspoding JSON documnet read and logge by the application
  ```
  g.addV('person').property('firstName', 'Thomas').property('lastName', 'Andersen').property('partitionkey', 'pk')
  ```
