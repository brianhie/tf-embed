from multiprocessing import Pool
import os
import sys
import subprocess

##############
# Parameters #

N_WORKERS = 8 # Threads.
FILE_SUFFIX = '.bed.gz'
WINDOWS_KB = int(sys.argv[1])

##############

def worker(tf, pos):
    # Debug output.
    if pos % 50 == 0:
        print('TF x {0}kb window: Processing TF {1}'
              .format(WINDOWS_KB, pos))

    command = (
        '/modules/pkgs/bedtools/2.25.0/bin/bedtools ' +
        'intersect -a {0} -b {1} -wa -c'
    ).format(
        'data/windows_{0}kb.bed'.format(WINDOWS_KB),
        'data/' + tf + FILE_SUFFIX
    )
    
    ps = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output = subprocess.check_output('cut -f 4'.split(),
                                     stdin=ps.stdout)
    # Ensure directory is created before writing to it.
    tf_dir = 'target/tf_by_window/{0}'.format(tf)
    os.makedirs(tf_dir, exist_ok=True)

    # Save data to file.
    with open(tf_dir + '/windows_{0}kb.tsv'
              .format(WINDOWS_KB), 'w+') as f:
        f.write(output.decode('utf-8'))

if __name__ == '__main__':
    tfs = sorted(
        [ os.path.join(dp, f)[:-len(FILE_SUFFIX)][len('data/'):]
          for dp, dn, filenames in os.walk('data/')
          for f in filenames if f.endswith(FILE_SUFFIX) ]
    )

    pool = Pool(processes=N_WORKERS)
    results = [
        pool.apply_async(worker, [ tf, pos ])
        for pos, tf in enumerate(tfs)
    ]
    ans = [ r.get() for r in results ]
