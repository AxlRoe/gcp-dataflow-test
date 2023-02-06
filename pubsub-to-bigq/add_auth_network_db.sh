#!/bin/bash

gcloud sql instances patch scraper-db --authorized-networks=$1 --quiet

