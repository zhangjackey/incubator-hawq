SELECT datname
FROM pg_catalog.pg_database
WHERE datname NOT IN ('template0', 'template1', 'postgres', 'hdfs','hcatalog')
ORDER BY datname;
