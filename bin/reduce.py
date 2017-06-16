import glob
import os
import sys

if __name__ == '__main__':
    with open(sys.argv[2], 'w+') as ofile:
        for filename in glob.iglob(sys.argv[1]):
            print('Processing file: ' + filename)
            with open(filename, 'r') as f:
                ofile.write(f.read().rstrip().replace('\n', '\t') + '\n')
     
