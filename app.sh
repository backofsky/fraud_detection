#/bin/bash

# create directory
mkdir -p archive

# remove database
# rm database.db

# unzip the file
unzip -o data.zip

# running scripts
python main.py