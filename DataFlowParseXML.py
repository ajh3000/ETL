#!/usr/bin/env python

import sys
import re
import argparse
import apache_beam as beam
import xml.etree.ElementTree as ET
from apache_beam import pvalue


def parse_and_move(path):
    import xml.etree.ElementTree as ET
    import re
    import sys
    import apache_beam as beam
    from apache_beam import pvalue
    try:
        open_file = beam.io.filesystems.FileSystems.open(path)
        content = open_file.read()
        root = ET.fromstring(content)
        root.findall(".")
        item_list = []
        for item in root.findall(".//channel/item"):
            link = item.find('link').text
            title = item.find('title').text
            pubdate = item.find('pubDate').text
            i = {
                "pubdate": pubdate,
                "link": link,
                "title": title
            }
            item_list.append(i)

        dest = re.sub(args.unprocessed_dir, args.processed_dir, path) 
        beam.io.filesystems.FileSystems.rename([path], [dest])

        yield pvalue.TaggedOutput('ok', item_list)
        yield item_list

    except Exception as e:
        #return [{"pubdate":"error","link":path,"title":str(e)}]
        yield pvalue.TaggedOutput('fail', str(e))
        yield (str(e), str(path))    


if __name__ == '__main__':

   parser = argparse.ArgumentParser()
   parser.add_argument('-ds', '--dataset', dest='dataset', action='store', help='target dataset name')
   parser.add_argument('-t', '--table', dest='table', action='store', help='table name')
   parser.add_argument('-p', '--project', dest='project', action='store', help='project name')
   parser.add_argument('-b', '--bucketpath', dest='bucketpath', action='store', help='temporary bucket path for processing')
   parser.add_argument('-pt', '--patterns', dest='patterns', action='store', help='pattern(s) of source XML file')
   parser.add_argument('-pd', '--processed_dir', dest='processed_dir', action='store', help='path to processed files')
   parser.add_argument('-ud', '--unprocessed_dir', dest='unprocessed_dir', action='store', help='path to unprocessed files')
   parser.add_argument('-r', '--runner', dest='runner', action='store', help='run method')

   args = parser.parse_args()

   OUTPUT_TABLE = args.project + ':' + args.dataset + '.' + args.table
   TABLE_SCHEMA = ('pubdate:STRING, link:STRING, title:STRING')


   argv = [
      '--project={0}'.format(args.project),
      '--job_name=parse-and-write',
      #'--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(args.bucketpath),
      '--temp_location=gs://{0}/staging/'.format(args.bucketpath),
      '--runner=DataflowRunner'
   ]

   p = beam.Pipeline(argv=argv)

   fmd_list =  beam.io.filesystems.FileSystems.match([args.patterns])
   path_list = []
   for i in fmd_list[0].metadata_list:
        path_list.append(i.path)
   print("Number of files to be processed: ", len(path_list))

   collection = (p | 'CreatePathList' >> beam.Create(path_list)
                   | 'ParseXML' >> beam.FlatMap(parse_and_move).with_outputs('ok', 'fail', main='main path')
                )

   (collection['ok'] | 'Flatten' >> beam.FlatMap(lambda x: x)  
                     | 'WriteToBigQuery' >> beam.io.gcp.bigquery.WriteToBigQuery(
                           custom_gcs_temp_location = 'gs://dsm-2018/dataflow/bigquery-temp',
                           table = OUTPUT_TABLE,
                           schema = TABLE_SCHEMA,
                           create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                           write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND)
   )

   p.run()

