#!/usr/bin/env python

import sys
import re
import argparse
import apache_beam as beam
import xml.etree.ElementTree as ET
from apache_beam.io.gcp import bigquery_file_loads as bqfl

unprocessed_folder = ''
processed_folder = 'processed'

def parse_and_move(path):
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

        dest = re.sub(unprocessed_folder, processed_folder, path) 
        beam.io.filesystems.FileSystems.rename([path], [dest])

        return item_list

    except Exception as e:
        return [{"pubdate":"error","link":path,"title":str(e)}]


if __name__ == '__main__':

   #PATH_PATTERNS = [ 'gs://your/path/pattern*.xml']


   parser = argparse.ArgumentParser()
   parser.add_argument('-ds', '--dataset', dest='dataset', 
        action='store', help='target dataset name')
   parser.add_argument('-t', '--table', dest='table', 
        action='store', help='table name')
   parser.add_argument('-p', '--project', dest='project', 
        action='store', help='project name')
   parser.add_argument('-b', '--bucketpath', dest='bucketpath', 
        action='store', help='temporary bucket path for processing')
   parser.add_argument('-pt', '--patterns', dest='patterns', 
        action='store', help='pattern(s) of source XML file')

   args = parser.parse_args()

   #OUTPUT_TABLE = 'PROJECT:DATASET.TABLE'
   OUTPUT_TABLE = args.project + ':' + args.dataset + '.' + args.table
   TABLE_SCHEMA = ('pubdate:STRING, link:STRING, title:STRING')


   argv = [
      '--project={0}'.format(args.project),
      '--job_name=parse-and-write',
      '--save_main_session',
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

   #dest_list = [re.sub('unprocessed','processed', x) for x in path_list ]

   (p
      | 'CreatePathList' >> beam.Create(path_list)
      | 'ParseXML' >> beam.FlatMap(lambda x: parse_and_move(x) )
      | 'WriteToBigQuery' >> beam.io.Write( beam.io.gcp.bigquery.BigQuerySink(
            table=OUTPUT_TABLE,
            schema=TABLE_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
   )

   p.run()

