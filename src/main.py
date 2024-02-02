import os
from concurrent import futures
import logging

import grpc
import abstractive_summarize_pb2
import abstractive_summarize_pb2_grpc


from summarizer import Summarizer

class AbstractiveSummarizer(abstractive_summarize_pb2_grpc.AbstractiveSummarizerServicer):
    def __init__(self):
        self.summarizer = Summarizer()

    def AbstractiveSummarize(self, request: abstractive_summarize_pb2.SummarizationRequest, context):
        summaries_list = self.summarizer.summarize(request.document, request.targets)
        summaries = []
        for summary in summaries_list:
            summaries.append(abstractive_summarize_pb2.Summary(target_uuid=summary["target_uuid"], summary=summary["summary"]))
        return abstractive_summarize_pb2.Summaries(summaries=summaries)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    abstractive_summarize_pb2_grpc.add_AbstractiveSummarizerServicer_to_server(AbstractiveSummarizer(), server)
    server.add_insecure_port('[::]:' + os.environ['SERVICE_PORT'])
    server.start()
    print(f"Server started, listening on {os.environ['SERVICE_PORT']}")
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
