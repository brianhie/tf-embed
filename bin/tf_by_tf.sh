#!/bin/bash
#PBS -N tf_by_tf
#PBS -q long
#PBS -o /home/brianhie/scripts_out/tf_by_tf.out
#PBS -e /home/brianhie/scripts_out/tf_by_tf.err
#PBS -l nodes=1:ppn=7
#PBS -l mem=1gb

cd /home/brianhie/tf_embed

python bin/tf_by_tf.py 7 # I.e., 7 threads.
