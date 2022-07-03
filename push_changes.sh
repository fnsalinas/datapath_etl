#!/bin/bash

cd /home/FABIO/etl

lt=$(timedatectl | grep -i "Local time")
commit_msg=$lt" Update json_builder files from github";

echo "Running git status...";
git status;

echo "Running git add...";
git add . -A;

echo "Running git commit... "$commit_msg;
git commit -m "$commit_msg";

echo "Running git push origin master...";
git push;

echo "Git push complete.";