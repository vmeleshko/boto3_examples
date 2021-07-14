import boto3

from logger import logger


def get_bucket_names():
    s3_resource = boto3.resource('s3')
    return [bucket.name for bucket in s3_resource.buckets.all()]


# def check_if_bucket_exist(name):
#     s3_resource = boto3.resource('s3')
#     for bucket in s3_resource.buckets.all():
#         if bucket.name == name:
#             return True
#     return False

def check_if_bucket_exist(name):
    return name in get_bucket_names()


def create_bucket(name: str) -> bool:
    if check_if_bucket_exist(name):
        logger.info(f"Bucket with name {name} already exists. No need to create it")
    else:
        try:
            bucket = boto3.resource('s3').Bucket(bucket_name)
            bucket.create()
        except Exception as e:
            logger.error(f"Create of bucket {name} failed with error: {e.__str__()}")
            return False
    return True


def upload_files_into_bucket(name, list_of_files_to_upload):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.upload_file
    s3_bucket = boto3.resource('s3').Bucket(name)
    counter = 0
    for f in list_of_files_to_upload:
        try:
            key = f.split('/')[-1]
            s3_bucket.upload_file(f, key)
            counter += 1
            logger.info(f'{f} has successfully uploaded into {name} bucket as {key} key')
        except Exception as e:
            logger.error(f'Upload of {f} into {name} bucket failed with error: {e.__str__()}')
    logger.info(f'{counter} files have been successfully uploaded into {name} bucket')
    return counter


def upload_objects_into_bucket(name, list_of_objects_to_upload):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.upload_fileobj
    s3_bucket = boto3.resource('s3').Bucket(name)
    counter = 0
    for obj in list_of_objects_to_upload:
        try:
            key = obj.split('/')[-1]
            with open(obj, 'rb') as data:
                s3_bucket.upload_fileobj(data, key)
            counter += 1
            logger.info(f'{obj} has successfully uploaded into {name} bucket as {key} key')
        except Exception as e:
            logger.error(f'Upload of {obj} into {name} bucket failed with error: {e.__str__()}')
    logger.info(f'{counter} objects have been successfully uploaded into {name} bucket')
    return counter


def list_bucket_objects(name):
    object_keys = []
    for bucket_object in boto3.resource('s3').Bucket(name).objects.all():
        object_keys.append(bucket_object.key)
        # print(bucket_object.key, dir(bucket_object))
    return object_keys


def delete_objects_from_bucket(name, list_of_objects_for_delete):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.delete_objects
    if list_of_objects_for_delete:
        objects_to_delete = {"Objects": [{'Key': obj} for obj in list_of_objects_for_delete]}
        try:
            boto3.resource('s3').Bucket(name).delete_objects(Delete=objects_to_delete)
            logger.info(f'Objects {list_of_objects_for_delete} successfully deleted from {name} bucket')
        except Exception as e:
            logger.error(f'Failed to delete objects from {name} bucket with error: {e.__str__()}')
    else:
        logger.warning('Empty list of objects for delete passed')


def delete_bucket(name):
    try:
        boto3.resource('s3').Bucket(name).delete()
        logger.info(f'Bucket {name} has successfully deleted')
    except Exception as e:
        logger.error(f'Failed to delete {bucket_name} bucket with error: {e.__str__()}')



bucket_name = 's3-vitalii-test'
list_of_files = ['fruits.txt', '/home/vitalii_meleshko/work/boto_examples/users.txt']
list_of_objects = ['/home/vitalii_meleshko/work/boto_examples/boto3_python.png']

# 1. Create bucket.
# 2. Upload files into bucket.
# 3. Upload object into bucket.
# 4. List Objects in the bucket.
# 5. Delete all objects in the bucket.
# 6. Delete bucket.

create_bucket(bucket_name)

if create_bucket(bucket_name):
    upload_files_into_bucket(bucket_name, list_of_files)
    upload_objects_into_bucket(bucket_name, list_of_objects)

    objects_in_bucket = list_bucket_objects(bucket_name)
    print(f'Bucket {bucket_name} has {len(objects_in_bucket)}: {objects_in_bucket}')

    # delete_bucket(bucket_name)
    #
    # delete_objects_from_bucket(bucket_name, objects_in_bucket)
    #
    # delete_bucket(bucket_name)

print(check_if_bucket_exist(bucket_name))