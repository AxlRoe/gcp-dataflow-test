#!/bin/bash

V=$(gcloud app versions list | grep $1 | tail -n 1 | awk '{print $2}')
gcloud app versions stop $V
