import base64
import csv
import json
import threading
import time
import timeit
import urllib2

import boto3
import requests
import slackweb

from my_queue import MyQueue

slack = slackweb.Slack(url='https://hooks.slack.com/services/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')


def notify(message):
    slack.notify(text="Adwords Scorer: {}".format(message), channel='#background-jobs', username='cahootsy-bot', icon_emoji=':snowboarder:')


class Scorer:
    def __init__(self):
        self.my_queue = MyQueue(24, self.on_process, self.on_error)
        self.file_lock = threading.Lock()
        self.out_file = open('out.csv', 'wb')

        fieldnames = ['ad_id', 'keywords', 'count', 'minimum', 'maximum', 'average', 'standard_deviation',
                      'cahootsy_url']
        self.out_csv = csv.DictWriter(self.out_file, fieldnames=fieldnames, dialect="excel")

        self.out_csv.writeheader()

        self.progress_bar = None
        self.current_percentage = 0
        self.counter = 0
        self.total = 0

    def run(self, source_file):
        notify('Starting Adwords Score Evaluation')

        reader = csv.DictReader(open(source_file, 'rU'), dialect='excel')

        self.my_queue.enqueue(lambda x: self.enqueue(reader, x))

        self.my_queue.run()

        self.out_file.close()

        if self.progress_bar is not None:
            self.progress_bar.close()

    def enqueue(self, reader, work_queue):
        for row in reader:
            self.total += 1

            work_queue.put({'solr_search_term': row['solr_search_term'], 'ad_id': (row['ad_id']), 'search_url': row['FinalURL']})

        notify('Importing {} Adword Score Evaluations'.format(self.total))
        # self.progress_bar = tqdm(total=self.total)

    def on_process(self, data):
        encoded_keywords = urllib2.quote(data['solr_search_term'])

        url = "http://localhost:8983/solr/cahootsy-sunspot/select?q={}" \
              "&fq=type%3AExternalProduct&fq=available_b%3Atrue" \
              "&fl=*+score&wt=json&defType=edismax&qf=name_texts+description_text+non_stem_name_text" \
              "&pf=name_texts%5E2.0&stopwords=true&lowercaseOperators=true&rows=1" \
              "&scoreDist=true".format(encoded_keywords)
        try:
            request = urllib2.Request(url)
            base64string = base64.encodestring('solr:5Dloo1QXyTqV').replace('\n', '')
            request.add_header("Authorization", "Basic %s" % base64string)
            results = json.loads(urllib2.urlopen(request).read())
        except (Exception) as e:
            print e
            print "Url failed: {} ({})".format(url, e)
            return
        finally:
            if self.progress_bar is not None:
                self.progress_bar.update(1)

            self.counter += 1

            if (self.counter * 100 / self.total) > (self.current_percentage + 5):
                self.current_percentage += 5
                notify("Import processed {}%".format(self.current_percentage))

        self.file_lock.acquire()
        try:
            if results['response']['numFound'] > 0:
                self.out_csv.writerow({
                    'ad_id': data['ad_id'],
                    'keywords': encoded_keywords,
                    'count': results['scoreStats']['numDocs'],
                    'minimum': results['scoreStats']['min'],
                    'maximum': results['scoreStats']['max'],
                    'average': results['scoreStats']['avg'],
                    'standard_deviation': results['scoreStats']['stdDev'],
                    'cahootsy_url': "https://www.cahootsy.com/compare?q={}&ad_id={}".format(encoded_keywords, data['ad_id'])
                })
        except KeyError as e:
            print url
            print results
            raise e

        finally:
            self.file_lock.release()

    def on_error(self, data):
        print "Error: {}".format(data)


boto3.setup_default_session(region_name='eu-west-1')

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
ec2 = boto3.resource('ec2')
ec2_client = boto3.client('ec2')

start = timeit.default_timer()

try:
    s3_client.download_file('cahootsy-production', 'adwords/source_urls.csv', 'urls.csv')

    Scorer().run('urls.csv')

    current_time = time.strftime('%Y-%m-%d-%H-%M-%S')
    file_name = 'adwords/output-{}.csv'.format(current_time)
    notify("Finished import. Uploading to [cahootsy-production]:{}".format(file_name))
    s3_client.upload_file('out.csv', 'cahootsy-production', file_name)

finally:
    response = requests.get('http://instance-data/latest/meta-data/instance-id')
    instance_id = response.text

    notify("Terminating instance {}".format(instance_id))
    ec2_client.terminate_instances(InstanceIds=[instance_id])

    stop = timeit.default_timer()

    m, s = divmod(stop - start, 60)
    h, m = divmod(m, 60)

    notify("Completed run in %d:%02d:%02d" % (h, m, s))
