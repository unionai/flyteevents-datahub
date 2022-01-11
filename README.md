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
                            Datahub server url, default=https://api.datahub.dev.aws.jpmchase.net                                                             

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
                            Datahub server url, default=https://api.datahub.dev.aws.jpmchase.net                                                                      
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

