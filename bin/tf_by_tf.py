from multiprocessing import Pool
import os
import sys
import subprocess

##############
# Parameters #

N_WORKERS = 7 # Threads.
FILE_SUFFIX = '.bed.gz'
LEFT_WINDOW = 1000 # Base pairs.
RIGHT_WINDOW = 1000 # Base pairs.

##############

def worker(tf, tfs):
    # Intersect TF with all other TFs.
    intersections = []
    for other_tf in tfs:
        if tf == '22Rv1/CTCF':
            print(other_tf)
        if tf == other_tf:
            intersections.append(0)
            continue

        command = (
            '/modules/pkgs/bedtools/2.25.0/bin/bedtools ' +
            'window -l {0} -r {1} -a {2} -b {3}'
        ).format(
            LEFT_WINDOW,
            RIGHT_WINDOW,
            'data/' + tf + FILE_SUFFIX,
            'data/' + other_tf + FILE_SUFFIX
        )
    
        ps = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output = subprocess.check_output('sort -u -k1,1 -k2,2n'.split(),
                                         stdin=ps.stdout)
        n_peaks = len(str(output).rstrip().split('\\n'))
        intersections.append(n_peaks)

    # Ensure directory is created before writing to it.
    tf_dir = 'target/tf_by_tf/{0}'.format(tf)
    os.makedirs(tf_dir, exist_ok=True)

    # Save data to file.
    with open(tf_dir + '/n_peaks.tsv', 'w+') as f:
        f.write('\n'.join([ str(i) for i in intersections ]))

if __name__ == '__main__':
    tfs = sorted(
        [ os.path.join(dp, f)[:-len(FILE_SUFFIX)][len('data/'):]
          for dp, dn, filenames in os.walk('data/')
          for f in filenames if f.endswith(FILE_SUFFIX) ]
    )

    pool = Pool(processes=N_WORKERS)
    results = [
        pool.apply_async(worker, [ tf, tfs ])
        for tf in tfs
    ]
    ans = [ r.get() for r in results ]
