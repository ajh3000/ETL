#!/usr/bin/env python

import sys
import re
import argparse
import apache_beam as beam
import xml.etree.ElementTree as ET
from apache_beam import pvalue


def parse_and_move(path_and_meta):
    import xml.etree.ElementTree as ET
    import re
    import sys
    import apache_beam as beam
    from apache_beam import pvalue
    try:
        path,_,_ = path_and_meta
        _,unprocessed_dir,_ = path_and_meta
        _,_,processed_dir = path_and_meta

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

        dest = re.sub(unprocessed_dir, processed_dir, path) 
        beam.io.filesystems.FileSystems.rename([path], [dest])

        yield pvalue.TaggedOutput('ok', item_list)
        yield item_list

    except Exception as e:
        error_pack = [{"filepath":path,"errormsg":str(e)}]
        yield pvalue.TaggedOutput('fail', error_pack)
        yield error_pack


if __name__ == '__main__':

   parser = argparse.ArgumentParser()
   parser.add_argument('-ds', '--dataset', dest='dataset', action='store', help='target dataset name')
   parser.add_argument('-t', '--table', dest='table', action='store', help='target table name')
   parser.add_argument('-et', '--error_table', dest='err_table', action='store', help='error table name')
   parser.add_argument('-p', '--project', dest='project', action='store', help='project name')
   parser.add_argument('-b', '--bucketpath', dest='bucketpath', action='store', help='temporary bucket path for processing')
   parser.add_argument('-pt', '--patterns', dest='patterns', action='store', help='pattern(s) of source XML file')
   parser.add_argument('-pd', '--processed_dir', dest='processed_dir', action='store', help='path to processed files')
   parser.add_argument('-ud', '--unprocessed_dir', dest='unprocessed_dir', action='store', help='path to unprocessed files')
   parser.add_argument('-sd', '--staging_dir', dest='staging_dir', action='store', help='path to staging directory')
   parser.add_argument('-r', '--runner', dest='runner', action='store', help='run method')
   parser.add_argument('-rg', '--region', dest='region', action='store', help='region where dataflow job runs')

   args = parser.parse_args()

   pattern_path = args.bucketpath + '/' + args.unprocessed_dir + '/' + args.patterns
   staging_path = args.bucketpath + '/' + args.staging_dir 
   temp_path = args.bucketpath + '/' + args.staging_dir 

   OUTPUT_TABLE = args.project + ':' + args.dataset + '.' + args.table
   TABLE_SCHEMA = ('pubdate:STRING, link:STRING, title:STRING')
   
   ERR_OUTPUT_TABLE = args.project + ':' + args.dataset + '.' + args.err_table
   ERR_TABLE_SCHEMA = ('filepath:STRING, errormsg:STRING')


   argv = [
      '--project={0}'.format(args.project),
      '--job_name=parse-and-write',
      #'--save_main_session',
      '--staging_location={0}/'.format(staging_path),
      '--temp_location={0}/'.format(staging_path),
      '--runner={0}'.format(args.runner),
      '--region={0}'.format(args.region)
   ]

   p = beam.Pipeline(argv=argv)

   fmd_list =  beam.io.filesystems.FileSystems.match([pattern_path])
   path_list = []
   for i in fmd_list[0].metadata_list:
        path_list.append((i.path, args.unprocessed_dir, args.processed_dir))
   print("Number of files to be processed: ", len(path_list))

   collection = (p | 'CreatePathList' >> beam.Create(path_list)
                   | 'ParseXML' >> beam.FlatMap(parse_and_move).with_outputs('ok', 'fail', main='main path')
                )

   (collection['ok'] | 'Flatten' >> beam.FlatMap(lambda x: x)  
                     | 'WriteToBigQuery' >> beam.io.gcp.bigquery.WriteToBigQuery(
                           table = OUTPUT_TABLE,
                           schema = TABLE_SCHEMA,
                           create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                           write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND)
   )
   
   (collection['fail'] | 'FlattenErrors' >> beam.FlatMap(lambda x: x)  
                       | 'WriteErrorsToBigQuery' >> beam.io.gcp.bigquery.WriteToBigQuery(
                             table = ERR_OUTPUT_TABLE,
                             schema = ERR_TABLE_SCHEMA,
                             create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                             write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND)
   )

   p.run()

