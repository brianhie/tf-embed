import glob
import os
import sys

if __name__ == '__main__':
    # Get list of TFs and filenames in sorted order by TF name.
    tfs_and_files = []
    for filename in glob.iglob(sys.argv[1]):
        tf_name = '/'.join(filename.split('/')[-3:-1])
        tfs_and_files.append((tf_name, filename))
    tfs_and_files = sorted(tfs_and_files)
    tfs = [ x[0] for x in tfs_and_files ]
    filenames = [ x[1] for x in tfs_and_files ]
            
    # Write actual data to file.
    with open(sys.argv[2], 'w+') as ofile:
        for filename in filenames:
            print('Processing file: ' + filename)
            with open(filename, 'r') as f:
                ofile.write(f.read().rstrip().replace('\n', '\t') + '\n')

    # Write meta data file including column labels.
    with open(sys.argv[2] + '.meta', 'w+') as ofile:
        ofile.write('\t'.join([ 'ROW', 'TF' ]) + '\n')
        for pos, tf_name in enumerate(tfs):
            # Filenames are, e.g., target/tf_by_tf/A549/POLR2A/n_peaks.tsv.
            ofile.write('\t'.join([ str(pos), tf_name ]) + '\n')
     
