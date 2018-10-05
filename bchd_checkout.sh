#!/bin/bash

git checkout $(cat $GOPATH/src/github.com/gcash/neutrino/glide.yaml | grep -A1 bchd | tail -n1 | awk '{ print $2}')
