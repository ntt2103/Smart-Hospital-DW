import json
from confluent_kafka import Consumer,TopicPartition
from operations.foundation import ROOT


def capture():
    c=Consumer({'bootstrap.servers':'kafka:9092','group.id':'hospital-raw-v1','enable.auto.commit':False})
    rows=[]
    try:
        for topic in ['appointments.events','encounters.events','laboratory.events']:
            for p in range(3):
                tp=TopicPartition(topic,p)
                _,end=c.get_watermark_offsets(tp,timeout=10)
                committed=c.committed([tp],timeout=10)[0].offset
                rows.append({'topic':topic,'partition':p,'high_watermark':end,'committed_offset':committed,'lag':end-max(committed,0)})
    finally:c.close()
    output={'partitions':rows,'total_lag':sum(r['lag'] for r in rows)}
    (ROOT/'artifacts/phase4/consumer_offsets.json').write_text(json.dumps(output,indent=2),encoding='utf-8')
    print(json.dumps({'total_consumer_lag':output['total_lag']}))
    return output


if __name__=='__main__':capture()
