#!/bin/bash

LL=$(grep -r 'gentle-nuance-343015' * | awk -F ':' '{print $1}' | uniq); for f in $LL; do sed -i 's/gentle-nuance-343015/gentle-nuance-343015/g' $f; done

