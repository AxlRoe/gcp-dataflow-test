#!/bin/bash

LL=$(grep -r 'scraper-v1-351921' * | awk -F ':' '{print $1}' | uniq); for f in $LL; do sed -i 's/scraper-v1-351921/scraper-v1-351921/g' $f; done

