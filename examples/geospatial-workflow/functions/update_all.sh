./create_zip.sh asc_to_geotiff.py
ibmcloud wsk action update geospatial-dag/asc_to_geotiff --docker aitorarjona/geospatial-dag-runtime:0.3 action.zip
./create_zip.sh get_stations.py
ibmcloud wsk action update geospatial-dag/get_stations --docker aitorarjona/geospatial-dag-runtime:0.3 action.zip
./create_zip.sh solar_radiance.py
ibmcloud wsk action update geospatial-dag/compute_solar_radiance --docker aitorarjona/geospatial-dag-runtime:0.3 action.zip
./create_zip.sh interpolate_wind.py
ibmcloud wsk action update geospatial-dag/interpolate_wind --docker aitorarjona/geospatial-dag-runtime:0.3 action.zip
./create_zip.sh interpolate_humidity.py
ibmcloud wsk action update geospatial-dag/interpolate_humidity --docker aitorarjona/geospatial-dag-runtime:0.3 action.zip
./create_zip.sh interpolate_temperature.py
ibmcloud wsk action update geospatial-dag/interpolate_temperature --docker aitorarjona/geospatial-dag-runtime:0.3 action.zip
./create_zip.sh gather_blocks.py
ibmcloud wsk action update geospatial-dag/gather_blocks --docker aitorarjona/geospatial-dag-runtime:0.3 action.zip
./create_zip.sh combine_calculations.py
ibmcloud wsk action update geospatial-dag/combine_calculations --docker aitorarjona/geospatial-dag-runtime:0.3 action.zip
