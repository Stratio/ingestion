#!/bin/bash

ansible-galaxy install --force -p roles $(<dependencies.txt)
