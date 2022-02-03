# lineage

Data lineage Python library

Initial support for Flyte -> (DataHub, AWS Glue Catalog)

## Lineage

Data lineage is extracted from Flyte by consuming events sent from FlyetAdmin during a workflow run. A workflow is ingested into one or more target systems.
Supported targets include [DataHub](https://datahubproject.io/) and the [AWS Glue Catalog](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog.html). 


### DataHub

The data model consists of entities: Datasets, Pipelines & Tasks 

Entites contain one or more aspects: Tags, Owners, etc

For example a pipeline can consist of many directed tasks with associated input and output datasets. The DataHub ingestion does not need to be captured in one pass by providing a complete pipeline data model as input. Individual entities can be ingested separately and wired together later by linking the entity ids (urns) together. Entity aspects can also be added as and when they become known in the system. This reflects reality where by an ingestion point only has local information such as the current workflow or task being processed and enables cross system dependencies to be wired together. 

### AWS Glue Catalog

Datasets are ingested into the Glue Catalog. A table is created for the dataset and optional database created to contain the table.


## Flight lineage server 

Flyte publishes SNS topics to an SQS queue for different workflow lifecycle events.
The events of interest for a workflow are grouped and processed to produce data lineage. The lineage includes the chain of workflow tasks and any cdatasets used within the flow. Only successfully run workflows are captured and datasets that are not fetched from the flyte cache.
The lineage is the ingested by the configured targets.

[traitlets](https://traitlets.readthedocs.io/en/stable/index.html) from Jupyter is used for the application configuration which can be configured within classes, as cmd line args or in a python config file (./flytelineage_config.py).

    $ flytelineage --help-all
    Flyte data lineage

    EventProcessor(Application) options                                                                            
    -----------------------------------                                                                            
    --EventProcessor.aws_region=<Unicode>                                                                          
        aws region                                                                                                 
        Default: 'us-east-1'                                                                                       
    --EventProcessor.config_file=<Unicode>                                                                         
        The config file to load                                                                                    
        Default: 'flytelineage_config.py'                                                                          
    --EventProcessor.log_datefmt=<Unicode>                                                                         
        The date format used by logging formatters for %(asctime)s                                                 
        Default: '%Y-%m-%d %H:%M:%S'                                                                               
    --EventProcessor.log_format=<Unicode>                                                                          
        The Logging format template                                                                                
        Default: '[%(name)s]%(highlevel)s %(message)s'                                                             
    --EventProcessor.log_level=<Enum>                                                                              
        Set the log level by value or name.                                                                        
        Choices: any of [0, 10, 20, 30, 40, 50, 'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL']                      
        Default: 30                                                                                                
    --EventProcessor.show_config=<Bool>                                                                            
        Instead of starting the Application, dump configuration to stdout                                          
        Default: False                                                                                             
    --EventProcessor.show_config_json=<Bool>                                                                       
        Instead of starting the Application, dump configuration to stdout (as JSON)                                
        Default: False                                                                                             
    --EventProcessor.sqs_queue=<Unicode>                                                                           
        sqs queue name or url                                                                                      
        Default: ''                                                                                                



                                                                                 

## Dataset lineage script

A script is available to ingest datasets into DataHub. The currently supported file types include parquet, csv and json.

    $ datasetlineage -h                                                                                                                                                 
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

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

