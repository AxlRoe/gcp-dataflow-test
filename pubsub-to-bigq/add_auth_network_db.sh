#!/bin/bash

gcloud sql instances patch scraper-db-3 --authorized-networks=$1 --quiet

