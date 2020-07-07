for file in $(ls functions/); do
	extension="${file##*.}"
	if [ "$extension" = "py" ] && [ "$file" != "cos_backend.py" ]; then
		cp functions/$file __main__.py
		zip z.zip functions/cos_backend.py __main__.py
		action="${file%.*}"
		ibmcloud wsk action update geospatial-dag/$action --docker aitorarjona/triggerflow-geospatial-runtime:0.4 -m 2048 -t 180000 z.zip
		rm z.zip __main__.py
	fi
done
