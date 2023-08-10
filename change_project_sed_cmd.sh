#!/bin/bash

LL=$(grep -r 'scraper-vx3' * | awk -F ':' '{print $1}' | uniq); for f in $LL; do sed -i 's/scraper-vx3/scraper-vx4/g' $f; done

