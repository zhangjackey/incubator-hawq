#!/bin/bash

# prep_GUCs.sh

# This script changes any default GUCs for optimal demo performance

hawq config -c max_connections -v 500
hawq config -c gp_segment_connect_timeout -v 600

hawq stop cluster -u
#hawq restart cluster -a
