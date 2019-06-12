# HOW TO RUN THIS PROJECT

To run this locally, you first have to start up a Zookeeper and Kafka server (step 1 and 2): https://kafka.apache.org/quickstart .
Next, create two topics, one called 'ledigingen' and one called 'stortingen'. These are the topics the Kafka producer
 will publish on.

You will also need a local postgres database running on port 5432. Create a table called container on the public schema 
with the data supplied under src/main/data/container_test_data.sql. 

Next, start the KafkaLedigingenConsumer and the KafkaStortingenConsumer. 

In order to run the KafkaProducer, add the following environment variables (e.g. in the run configuration of the class):
    - PASSWORD (the password to your local postgres)
    - USERNAME (the username for your local postgres)
    - DATABASE_NAME (the name of the postgres database)

Finally, when the data is processed, we can run the SingleFileFactory. 

You can train the Machine Learning model in the MachineLearningModelTrainer class. The trained Machine Learning model
 will be available in the target/tmp folder. This model is used in the final stage of this project, the 
MachineLearingService. The output says whether or not the container has to be emptied the day after, based on the 
trained model. 

