# abstractive-summary-service
Service that generates activity summaries for specified entities from a document.

##  Model
Specify the path of your summary model in `.env` (defaults to ```/abstractive-summary/models/```). You can download the model [here](https://drive.google.com/drive/folders/1NA-vJNwEDNs1NMrM1iGm6DylsbcgwKt3?usp=sharing). Place it in ```/models/``` for building.

##  Building
Build the image using ```build/Dockerfile```. This will generate 3 files with the abstractive_summarize_pb2* prefix in the /src folder.

## How to Serve
Run ```src/main.py``` to start gRPC server. Port can be configured via environment variable.
    
## How to Consume
Service exposes a gRPC method named AbstractiveSummarize. See ```test/test.py``` for an example of how to create a client to consume this service. Note that you will need the 3 files with the abstactive_summarize_pb2* prefix in your client source code for it to work.

