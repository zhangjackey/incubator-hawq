#!/bin/sh

source ~/.bashrc
source ~/qa.sh


hawq config -c hawq_rm_rejectrequest_nseg_limit -v 0
hawq config -c hawq_rm_tolerate_nseg_limit -v 0
hawq restart cluster -a -M fast

psql -d postgres -c "select * from gp_segment_configuration;" >config
hawqconfig -s default_hash_table_bucket_number >>config
hawqconfig -s hawq_rm_nslice_perseg_limit >>config
hawqconfig -s hawq_rm_rejectrequest_nseg_limit  >>config
hawqconfig -s hawq_rm_tolerate_nseg_limit >>config


