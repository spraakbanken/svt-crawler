#!/usr/bin/env bash

# Wrapper script for annotating all corpora with Sparv

# Start preloader (if not running already):
# nohup sparv preload -j8 > preloader.out 2>&1 &

# Run script:
# nohup ./annotate.sh > annotate.out 2>&1 &

# Increase chance for a process to be finished when machine runs out of memory
echo 500 > /proc/$$/oom_score_adj

corpora="
svt-2004
svt-2005
svt-2006
svt-2007
svt-2008
svt-2009
svt-2010
svt-2011
svt-2012
svt-2013
svt-2014
svt-2015
svt-2016
svt-2017
svt-2018
svt-2019
svt-2020
svt-2021
svt-2022
svt-nodate
"
for corpus in $corpora
do
  echo ""
  echo "------ Annotating corpus $corpus ------"
  cd $corpus
  sparv run -j8 --socket ../sparv.socket
  cd ..
  echo "------ Done annotating corpus $corpus ------"
done
