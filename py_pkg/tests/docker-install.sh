#!/bin/sh

apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv 46BD98A354BA5235

echo "deb https://dl.bintray.com/rvernica/deb trusty universe" \
     > /etc/apt/sources.list.d/bintray.list

apt-get update
apt-get install --assume-yes libarrow-dev
