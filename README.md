# lineage

Data lineage Python library

Initial support for Flyte -> DataHub

## Lineage

[DataHub](https://datahubproject.io/) is our target data cataloging system. 

The data model consists of entities: Datasets, Pipelines & Tasks 

Entites contain one or more aspects: Tags, Owners, etc

For example a pipeline can consist of many directed tasks with associated input and output datasets. The DataHub ingestion does not need to be captured in one pass by providing a complete pipeline data model as input. Individual entities can be ingested separately and wired together later by linking the entity ids (urns) together. Entity aspects can also be added as and when they become known in the system. This reflects reality where by an ingestion point only has local information such as the current task being processed and enables cross system dependencies to be wired together. 


## Flight lineage server 

Flyte can publish SNS topics to an SQS queue for different workflow lifecycle events.
The contents of these events are used to update our meta data catalog DataHub with workflow
data lineage from Flyte. The lineage includes the chain of workflow tasks and any cached datasets used within the flow. Only successfully run workflows are captured and datasets that are being cached for the first time.



    $ flyte_lineage -h                                                                                                              
    usage: flyte_lineage [-h] [-c FILE] [--sqs_queue SQS_QUEUE] [--aws_region AWS_REGION] [--datahub_server DATAHUB_SERVER]         
                                                                                                                                    
    Emit Flyte data lineage -> DataHub                                                                                              
                                                                                                                                    
    optional arguments:                                                                                                             
    -h, --help            show this help message and exit                                                                         
    -c FILE, --config FILE                                                                                                        
                            Path to configuration file (defaults to $CWD/etc/dev.ini)                                               
    --sqs_queue SQS_QUEUE                                                                                                         
                            SQS queue name, default=103020-flyte-events                                                             
    --aws_region AWS_REGION                                                                                                       
                            AWS region, default=us-east-1                                                                           
    --datahub_server DATAHUB_SERVER                                                                                               
                            Datahub server url, e.g. https://api.datahub.dev.aws.great.net  
    --emit EMIT           Emit lineage to DataHub, default=True
    --datasets-only DATASETS_ONLY
                        Process datasets only not pipeline task lineage, default=False                                                                                    

## dataset_lineage script

A script is available to ingest datasets into DataHub. The currently supported file types include parquet, csv and json.

    $ dataset_lineage -h                                                                                                                                                 
    usage: emit_dataset [-h] [-p PLATFORM] [-s SERVER] [-f FILEPATH] [--filetype FILETYPE] [-n NAME] [-d DESCRIPTION] [-o OWNERS] [-t TAGS] [-c FILE]                 
                                                                                                                                                                    
    Emit Source Dataset -> Datahub                                                                                                                                                   
                                                                                                                                                                    
    optional arguments:                                                                                                                                               
    -h, --help            show this help message and exit                                                                                                           
    -p PLATFORM, --platform PLATFORM                                                                                                                                
                            Source platform, default=flyte                                                                                                            
    -s SERVER, --server SERVER                                                                                                                                      
                            Datahub server url, e.g. https://api.datahub.dev.aws.great.net                                                                      
    -f FILEPATH, --filepath FILEPATH                                                                                                                                
                            dataset filepath                                                                                                                          
    --filetype FILETYPE   file type format, one of csv, json or parquet                                                                                             
    -n NAME, --name NAME  dataset name                                                                                                                              
    -d DESCRIPTION, --description DESCRIPTION                                                                                                                       
                            dataset description                                                                                                                       
    -o OWNERS, --owners OWNERS                                                                                                                                      
                            comma separated list of owners e.g. joe,clair                                                                                             
    -t TAGS, --tags TAGS  comma separated list of tags e.g. wonderful,beauty                                                                                        
    -c FILE, --config FILE                                                                                                                                          
                            Path to configuration file (defaults to $CWD/etc/dev.ini)                                                                                 
                                                                                                                                                                                                                             

## Development

    $ git clone https://github.com/unionai/flyteevents-datahub.git
    $ cd flyteevents-datahub
    $ python -m venv env                                                                                
    $ source env/bin/activate                                                                      
    $ pip install -r requirements.txt
    $ python setup.py develop

### Debugging

Manually get message from the queue

    $ aws sqs receive-message --queue-url 103020-flyte-events                                                                                                                                               
    {                                                                                                                                                                                                       
        "Messages": [                                                                                                                                                                                       
            {                                                                                                                                                                                               
                "Body": "{\n  \"Type\" : \"Notification\",\n  \"MessageId\" : \"8b2aa9ea-7490-54c4-9bf3-f3b072070be0\",\n  \"TopicArn\" : \"arn:aws:sns:us-east-1:965012431333:103020-flyte-raw-event\",\n  
    ",\n  \"Message\" : \"EtIECjsIARIDcG9jGgtkZXZlbG9wbWVudCIgbmV3cy53b3JrZmxvd3MuY292aWQuZmlsdGVyX2RhdGEqA3YyMxIkCgJuMRIeCgNwb2MSC2RldmVsb3BtZW50IgpnaXQ3cTRxZGszIAIqCXByb3BlbGxlcjKvAgqSAmh0dHBzOi8vY29uc2
    zLWVhc3QtMSNsb2dFdmVudFZpZXdlcjpncm91cD0vYXdzL2Vrcy9mbHl0ZS1wb2MxLWZseXRlLWNsdXN0ZXIvY29udGFpbmVyczoqO3N0cmVhbT12YXIubG9nLmNvbnRhaW5lcnMuZ2l0N3E0cWRrMy1uMS0wX3BvYy1kZXZlbG9wbWVudF9naXQ3cTRxZGszLW4xLTA
    ZjVlNGU5YjcyYTBhOTIyYzM2OTcxZi5sb2cSFkNsb3Vkd2F0Y2ggTG9ncyAoVXNlcikYAjoLCNeIgI8GENuM1QhCc3MzOi8vYXBwLWlkLTEwMzAyMC1kZXAtaWQtMTAzMDIxLXV1LWlkLTVxcXppdmtoNXlhai9tZXRhZGF0YS9wcm9wZWxsZXIvcG9jLWRldmVsb3Bt
    XRhc2uCARwKD2dpdDdxNHFkazMtbjEtMCIJY29udGFpbmVy\",\n  \"Timestamp\" : \"2022-01-13T10:52:07.032Z\",\n  \"SignatureVersion\" : \"1\",\n  \"Signature\" : \"RLnLJEO81UCebls1U/ke6NMUIpsQHPip6GPVLbb+2dFpI9
    fM8kdhUwVH+CU4eX/alDepfrWP/hPrxXZO+cheVfqfjATdjHXH/CIgNtyUHIW3jyVHRjT+8/CGDmUwvEoV9xfT7L5tjfQUFs/pu5ov4MP6bUN3gvoXIkyKBOdATlJHqIZ5/QdbBJvUdo6xE9f2kz8x1VUQHXj8zXhU8T7kXFQDqgwbu3zUHMF32tkNkGLA8CKPqazILA
    CertURL\" : \"https://sns.us-east-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem\",\n  \"UnsubscribeURL\" : \"https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&Su
    lyte-raw-event:7e33b55c-3065-4948-906a-c7c6815d7aa8\"\n}",                                                                                                                                              
                "ReceiptHandle": "AQEBVGrrv48JVIR4AvCtoIa0uR+jcr99fK70TkukY+tcYYHQorNkuako9MvqhLoDh9X77gymQExN7KSfdN//zQ3S0MX129p9gbbQwA1YgoNhbVPMFEuQr9YtZHbxyVoDS1N5FZQH+8tXMsEDQOMalR4ZH9zylWBus9dq1Z1IRJ
    fyAaPRn8qa6wunEMadTxOuslDaAvbKqDhqjiww9DIgjQF5hI/kKjbKqZiCyhxuuo7O8iCqYRjvczMbgW0aU5lAoL3a91ZfkMPJbDwTYe5iodQt19Ktk97a2iYtFDUydFTeoqraRI+zpHyvJZ6kOAdI9X9xLmgh0DZo4AINEv5AvUhVyFNk4J2og==",             
                "MD5OfBody": "72ec9350469876d18135b5c5d17b8b9f",                                                                                                                                            
                "MessageId": "b073b37f-e569-454e-a3bb-27708a5a6237"                                                                                                                                         
            }                                                                                                                                                                                               
        ]                                                                                                                                                                                                   
    }                                                                                                                                                                                                       


## GitHub 

### Worflow

Fork the project from https://github.com/unionai/flyteevents-datahub


Keep your fork by tracking the original "upstream" repo that you forked

    $ git remote add upstream https://github.com/unionai/flyteevents-datahub.git
    $ git remote -v
    origin  https://github.com/erowan/flyteevents-datahub.git (fetch)
    origin  https://github.com/erowan/flyteevents-datahub.git (push)
    upstream  https://github.com/unionai/flyteevents-datahub.git (fetch)
    upstream  https://github.com/unionai/flyteevents-datahub.git (push) 


Whenever you want to update your fork with the latest upstream changes, you'll need to first fetch the upstream repo's branches and latest commits to bring them into your repository:

    # Fetch from upstream remote
    git fetch upstream

    # View all branches, including those from upstream
    git branch -va

    Now, checkout your own main branch and merge the upstream repo's main branch:

    # Checkout your main branch and merge upstream
    git checkout main
    git merge upstream/main

Doing Your Work - Create a Branch  

### commiting

As this is a public repo you need need to generate a token from your github account and use that for the password. 

Remember to change to your personal user info

    $ git config user.name "joe"
    $ git config user.email "joe@lovely.com"





##  Testing

    $ pip install -r requirements-dev.txt
    $ pytest -v
    
    # Run tests with coverage reporting
    $ pytest --cov  
    $ pytest --cov=lineage --cov-report term-missing lineage 

    # Run tests with logging
    $ pytest --log-level=DEBUG 


##  Style

    $ black .

