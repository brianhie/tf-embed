from multiprocessing import Pool
import os
import sys
import subprocess

##############
# Parameters #

N_WORKERS = int(sys.argv[1]) # Threads.
FILE_SUFFIX = '.bed.gz'
WINDOW = 1000 # Base pairs.

##############

def write_intersect_files(tf, other_tf, tfs):
    tf_by_otf = ('target/tf_by_tf/{0}/intersect_{1}.bed'
                 .format(tf, other_tf.replace('/', '_')))
    with open(tf_by_otf, 'w+') as tf_by_otf_file:
        ps1 = subprocess.Popen(
            ('/modules/pkgs/bedtools/2.25.0/bin/bedtools ' +
             'intersect -u -a {0} -b {1}'
             .format(
                 'data/' + tf + FILE_SUFFIX,
                 'data/' + other_tf + FILE_SUFFIX
             )).split(),
            stdout=tf_by_otf_file
        )
        
    otf_by_tf = ('target/tf_by_tf/{1}/intersect_{0}.bed'
                 .format(tf.replace('/', '_'), other_tf))
    with open(otf_by_tf, 'w+') as otf_by_tf_file:
        ps2 = subprocess.Popen(
            ('/modules/pkgs/bedtools/2.25.0/bin/bedtools ' +
             'intersect -u -a {1} -b {0}'
             .format(
                 'data/' + tf + FILE_SUFFIX,
                 'data/' + other_tf + FILE_SUFFIX
             )).split(),
            stdout=otf_by_tf_file
        )

    ps1.wait()
    ps2.wait()
        
    return tf_by_otf, otf_by_tf

def worker(tf, tfs):
    # Ensure directory is created before writing to it.
    tf_dir = 'target/tf_by_tf/{0}'.format(tf)
    os.makedirs(tf_dir, exist_ok=True)

    # Intersect TF with all other TFs.
    intersections = []
    for pos, other_tf in enumerate(tfs):
        if tf == other_tf:
            # Debug output.
            print('TF x TF: Processing TF {0}'
                  .format(pos))
            # TF intersected with itself is 0.
            intersections.append(0)
            continue
        elif tf > other_tf:
            # Will result in upper triangular TF x TF matrix.
            intersections.append(0)
            continue

        # Write individual BED files with only overlapping peaks.
        tf_by_otf, otf_by_tf = write_intersect_files(tf, other_tf, tfs)

        # Then, concatenate the two files, sort, ...
        command = 'sort -k1,1 -k2,2n {0} {1}'.format(tf_by_otf, otf_by_tf)
        ps = subprocess.Popen(command.split(), stdout=subprocess.PIPE)

        # ... and merge.
        command = (
            '/modules/pkgs/bedtools/2.25.0/bin/bedtools ' +
            'merge -d {0} -i stdin'
        ).format(WINDOW)
        output = subprocess.check_output(command.split(),
                                         stdin=ps.stdout)
        ps.wait()
        n_peaks = len(output.decode('utf-8').rstrip().split('\n'))
        intersections.append(n_peaks)

        # Delete files once done.
        command = 'rm -f {0} {1}'.format(tf_by_otf, otf_by_tf)
        subprocess.Popen(command.split()).wait()

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
