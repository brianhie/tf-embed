from datetime import datetime
from multiprocessing import Pool
import os
import sys
import subprocess

N_WORKERS = 12

def worker(commands):
    for command in commands:
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        process.wait()

if __name__ == '__main__':
    tf_to_commands = {}
    tf_to_date = {}
    for line in open(sys.argv[1], 'r'):
        fields = line.rstrip().split('\t')
    
        # Extract tf and remove '-human' suffix.
        tf = fields[16]
        if tf.endswith('-human'):
            tf = tf[:-len('-human')]
    
        # Various filters not available in web UI.
        output_type = fields[2]
        if output_type != 'optimal idr thresholded peaks':
            continue
        file_format = fields[1]
        if file_format != 'bed narrowPeak':
            continue
        assembly = fields[42]
        if assembly != 'hg19':
            continue
        file_status = fields[45]
        if file_status != 'released':
            continue
        biosample_treatment = fields[12]
        if biosample_treatment != '':
            continue

        # Filter out bad quality files.
        if len(fields) > 48:
            error_internal_action = fields[48]
            if error_internal_action != '':
                continue
        if len(fields) > 49:
            error = fields[49]
            if error != '':
                continue

        # Separate files by cell line.
        cell_line = fields[6]
        cell_line = cell_line.replace('/', '-').replace(' ', '_')
        if not os.path.isdir('data/' + cell_line):
            os.mkdir('data/' + cell_line)

        # Keep the most recent sample.
        date = datetime.strptime(fields[24], '%Y-%m-%d')
        tf_id = cell_line + '/' + tf
        if tf_id in tf_to_date:
            if tf_to_date[tf_id] < date:
                tf_to_date[tf_id] = date
            else:
                continue
        else:
            tf_to_date[tf_id] = date

        # Create download command.
        url = fields[41]
        url_file = url.split('/')[-1]
        tf_to_commands[tf_id] = [
            'wget ' + url,
            'mv {0} data/{1}/{2}.bed.gz'.format(url_file, cell_line, tf)
        ]

    # Download in parallel.
    pool = Pool(processes=N_WORKERS)
    results = [ pool.apply_async(worker, [ tf_to_commands[t] ]) for t in tf_to_commands ]
    ans = [ r.get() for r in results ]
