import subprocess
import boto3
import os
import gzip
import hashlib
from concurrent import futures

def lambda_handler(event, context):
    bucket, region = os.getenv('BUCKET'), os.getenv('REGION')
    target_dir = os.getenv('TARGET_DIR')
    prefix = event['prefix'].rstrip('/')
    images_tbl_key = event['images_tbl_key']
    print(images_tbl_key)

    s3 = boto3.client('s3', region_name=region)

    s3.download_file(bucket, '/'.join([prefix, images_tbl_key]), os.path.join(target_dir, images_tbl_key))

    image_keys = []

    with open(os.path.join(target_dir, images_tbl_key), 'r') as images_tbl_file:
        datatype = images_tbl_file.readline()
        columns = list(filter(lambda s: s.strip(), images_tbl_file.readline().split('|')[1:]))
        types = list(filter(lambda s: s.strip(), images_tbl_file.readline().split('|')[1:]))

        for line in images_tbl_file.readlines():
            data = line.split()
            image_keys.append({
                'image': data[-1].rstrip('.gz'),
                'key': data[-1],
                'url': data[-2]
            })
    images = {image['image'] for image in image_keys}

    files = os.listdir(target_dir)
    missing_images_keys = images.difference(files)
    download_images = [image for image in image_keys if image['image'] in missing_images_keys]
    print('Missing images: {}'.format(download_images))

    def download(image):
        command = ['/var/task/bin/mArchiveGet', '{}'.format(image['url']), image['key']]
        print(command)
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, cwd='/tmp')
        output, error = proc.communicate()
        print(output, error)

        zip_image = gzip.GzipFile(os.path.join('/', 'tmp', image['key']), 'rb')
        md5 = hashlib.md5()
        with open(os.path.join(target_dir, image['image']), 'wb') as image_file:
            image_bin = zip_image.read()
            md5.update(image_bin)
            image_file.write(image_bin)
        zip_image.close()
        os.remove(os.path.join('/', 'tmp', image['key']))
        print('md5({}) = {}'.format(image['image'], md5.hexdigest()))

    with futures.ThreadPoolExecutor() as pool:
        fut = []
        for image_key in download_images:
            f = pool.submit(download, image_key)
            fut.append(f)
        [f.result() for f in fut]
