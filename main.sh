#!/bin/bash

# Fill the data/ directory with raw peak data.
python bin/download.py resources/metadata.tsv

# Generate TF by TF peak overlap count data.
#
# There are a lot of file reads happening, so
# this takes a bit of time now (overnight). May
# want to optimize further by storing in memory,
# etc.
#
# Also see the script bin/tf_by_tf.sh for
# a bash script that can be submitted to the
# PBS cluster.
python bin/tf_by_tf.py

# Generate genomic windows files.
bedtools makewindows \
         -g resources/hg19.chrom.sizes.txt \
         -w 100000 \
         > data/windows_100kb.bed
bedtools makewindows \
         -g resources/hg19.chrom.sizes.txt \
         -w 1000000 \
         > data/windows_1000kb.bed

# Generate TF by genomic window data.
python bin/tf_by_window.py 100 # 100 KB.
python bin/tf_by_window.py 1000 # 1 MB.

# Put all the processed files into a single
# matrix file.
python bin/reduce.py \
       "target/tf_by_tf/*/*/n_peaks.tsv" \
       target/tf_by_tf/reduced.tsv
python bin/reduce.py \
       "target/tf_by_window/*/*/windows_100kb.tsv" \
       target/tf_by_window/window_100kb_reduced.tsv
python bin/reduce.py \
       "target/tf_by_window/*/*/windows_1000kb.tsv" \
       target/tf_by_window/window_1000kb_reduced.tsv
