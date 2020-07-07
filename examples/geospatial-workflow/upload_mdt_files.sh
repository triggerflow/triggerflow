for file in $(ls mdt/); do
	ibmcloud cos upload --bucket $BUCKET --key $file --file mdt/$file
done
