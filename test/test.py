import os
import logging

import grpc
import abstractive_summarize_pb2
import abstractive_summarize_pb2_grpc

SAMPLE_DOCUMENT = """Russian President Vladimir Putin has met his commanders in two regions of Ukraine that Moscow claims to have annexed, while Russian forces stepped up heavy artillery bombardments and air strikes on Tuesday on the devastated Ukrainian city of Bakhmut.
Ukraine's President Volodymyr Zelenskiy, meanwhile, visited troops in the eastern town of Avdiivka, about 70 km (43 miles) southwest of Bakhmut, and was briefed on the battlefield situation, his office said.
The Kremlin said Putin on Monday had attended a military command meeting in Ukraine's southern Kherson region and visited a national guard headquarters in eastern Luhansk.
Putin heard reports from commanders of the airborne forces and the Dnieper army group as well as other senior officers who briefed him on the situation in the Kherson and Zaporizhzhia regions in the south.
Neither Defence Minister Sergei Shoigu nor Chief of the General Staff Valery Gerasimov joined Putin on his trip as a security precaution, the Kremlin said.
A senior Ukrainian presidential aide, Mykhailo Podolyak, took to Twitter to mock Putin's trip as a "'special tour' of the mass murders' author in the occupied and ruined territories to enjoy the crimes of his minions for the last time".
Kyiv and the West accuse Russian forces of committing war crimes in occupied Ukrainian territory, which Moscow denies.
Kherson, Zaporizhzhia, Luhansk and Donetsk are the four regions that Putin proclaimed annexed last September following what Ukraine said were sham referendums. Russian forces only partly control the four regions.
Russian troops retreated from Kherson city, the regional capital, last November, and have been reinforcing their positions on the opposite bank of the Dnipro River in anticipation of a Ukrainian counteroffensive this spring.
While numerous Western leaders have made their way to Kyiv for talks with President Zelenskiy since Russian forces invaded 14 months ago, Putin has rarely visited parts of Ukraine under Russian control.
Last month, he visited Crimea - annexed by Russia in 2014 - and the southeastern city of Mariupol in Donetsk region.
A Russian winter offensive failed to make much progress and its troops have been bogged down in a series of battles in the east and south, where advances have been incremental and come at a huge cost to both sides.
Fighting has raged in and around Bakhmut in Donetsk region for months, with Ukrainian forces holding out despite regular claims by Russia to have taken the city.
"Currently, the enemy is increasing the activity of heavy artillery and the number of air strikes, turning the city into ruins," the commander of Ukraine's ground forces, General Oleksandr Syrskyi, said on Tuesday.
Bakhmut's capture could provide a stepping stone for Russia to advance on two bigger cities it has long coveted in the Donetsk region - Kramatorsk and Sloviansk.
The head of the Wagner mercenary group, which has spearheaded Russia's attempt to take Bakhmut, said this month that its fighters controlled more than 80% of the city. Ukraine's military has denied this.
Russia says its "special military operation" in Ukraine, launched on Feb. 24 last year was necessary to protect its security against what it sees as a hostile and aggressive West.
Ukraine and its Western allies say Russia is waging an unprovoked war aimed at grabbing territory.
On his visit to Avdiivka on Tuesday, Zelenskiy's office said he had handed out awards to his troops, telling them: "I have the honour to be here today, to thank you for your service, for defending our land, Ukraine, our families."
A meeting of Group of Seven foreign ministers in Japan condemned on Tuesday a Russian plan to station shorter-range, so-called tactical nuclear weapons in Belarus, a Moscow ally which borders Ukraine.
In a communique at the end of a three-day meeting in Japan, G7 foreign ministers said: "Russia's irresponsible nuclear rhetoric and its threat to deploy nuclear weapons in Belarus are unacceptable."
"Any use of chemical, biological or nuclear weapons by Russia would be met with severe consequences," they said.
The G7 groups the United States, Japan, Germany, Britain, France, Italy and Canada, which have all imposed economic sanctions on Russia over its invasion of Ukraine.
The war in Ukraine has killed tens of thousands of people, levelled cities, forced millions from their homes and shaken up the global security order, including by prompting Russia to strengthen ties with non-Western actors, especially China.
Russia's Defence Minister Shoigu told his Chinese counterpart Li Shangfu during talks in Moscow on Tuesday that their countries' military cooperation was a "stabilising" force in the world and helped to reduce the chances of conflict.
Li said his trip aimed to show the world that China firmly intends to strengthen its strategic cooperation with Russia, the TASS news agency reported.
Beijing has refrained from criticising Putin's invasion of Ukraine.
Meanwhile, the Japanese defence ministry said it had scrambled a jet fighter in response to what it said were Russian aircraft gathering information over seas near Japan. Earlier, Russia said two of its strategic bombers - which are capable of carrying nuclear warheads - had conducted patrol flights over the Sea of Okhotsk and the Bering Sea in Russia's Far East.
"""

def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel(f"localhost:{os.environ['SERVICE_PORT']}") as channel:
        stub = abstractive_summarize_pb2_grpc.AbstractiveSummarizerStub(channel)
        targets = [abstractive_summarize_pb2.Target(target_uuid=9999, span_start=18, span_end=32), # Vladimir Putin
                   abstractive_summarize_pb2.Target(target_uuid=9999, span_start=0, span_end=32), # Russian President Vladimir Putin (should select longer mention)
                   abstractive_summarize_pb2.Target(target_uuid=9988, span_start=74, span_end=81), # Ukraine
                   abstractive_summarize_pb2.Target(target_uuid=9988, span_start=251, span_end=258), # Ukraine (should select most common mention)
                   abstractive_summarize_pb2.Target(target_uuid=9988, span_start=251, span_end=260)] # Ukraine's
        response = stub.AbstractiveSummarize(abstractive_summarize_pb2.SummarizationRequest(document=SAMPLE_DOCUMENT, targets=targets))
        for summary in response.summaries:
            print(summary.target_uuid, summary.summary)

if __name__ == '__main__':
    logging.basicConfig()
    run()
