import time
import random
from requests.exceptions import HTTPError
from cloudant.client import Cloudant
from cloudant.database import CloudantDatabase
from cloudant.document import Document
from cloudant.error import CloudantException


class CloudantClient:
    max_retries = 15

    def __init__(self, cloudant_user: str, auth_token: str, url: str):
        self.client = Cloudant(cloudant_user=cloudant_user, auth_token=auth_token, url=url)
        self.client.connect()

    def get_conn(self):
        return self.client

    def put(self, database_name: str, document_id: str, data: dict):
        db = CloudantDatabase(self.client, database_name)
        data = data.copy()
        data['_id'] = document_id
        retry = self.max_retries
        while retry > 0:
            try:
                if not db.exists():
                    db.create()
                doc = Document(db, document_id=document_id)
                if doc.exists():
                    doc.fetch()
                    doc.delete()
                doc = db.create_document(data)
                doc.save()
                break
            except (CloudantException, HTTPError) as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e

    def get(self, database_name: str, document_id: str = None):
        db = CloudantDatabase(self.client, database_name)

        retry = self.max_retries
        while retry > 0:
            try:
                if not db.exists():
                    raise KeyError("Database does not exist")
                break
            except (CloudantException, HTTPError) as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e

        retry = self.max_retries
        while retry > 0:
            try:
                if document_id is None:
                    doc = dict()
                    for d in db:
                        task_id = d['_id']
                        del d['_id']
                        del d['_rev']
                        doc[task_id] = d
                else:
                    doc = Document(db, document_id=document_id)
                    if not doc.exists():
                        raise KeyError("Database or document does not exist")
                    doc.fetch()
                    del doc['_id']
                    del doc['_rev']
                break
            except (CloudantException, HTTPError) as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e
        return dict(doc)

    def delete(self, database_name: str, document_id: str = None):
        db = CloudantDatabase(self.client, database_name)
        retry = self.max_retries
        while retry > 0:
            try:
                if document_id is None and db.exists():
                    db.delete()
                else:
                    doc = Document(db, document_id=document_id)
                    doc.fetch()
                    if doc.exists():
                        doc.delete()
                break
            except (CloudantException, HTTPError) as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e

    def database_exists(self, database_name):
        db = CloudantDatabase(self.client, database_name)
        return db.exists()

    def document_exists(self, database_name, document_id):
        db = CloudantDatabase(self.client, database_name)
        retry = self.max_retries
        while retry > 0:
            try:
                doc = Document(db, document_id=document_id)
                doc.fetch()
                return doc.exists()
            except (CloudantException, HTTPError) as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e

    def key_exists(self, database_name, document_id, key):
        db = CloudantDatabase(self.client, database_name)
        retry = self.max_retries
        while retry > 0:
            try:
                doc = Document(db, document_id=document_id)
                doc.fetch()
                return key in doc
            except (CloudantException, HTTPError) as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e

    def set_key(self, database_name, document_id, key, value):
        retry = self.max_retries
        while retry > 0:
            db = CloudantDatabase(self.client, database_name)
            try:
                doc = Document(db, document_id=document_id)
                doc.update_field(
                    doc.field_set,
                    field=key,
                    value=value
                )
                #doc.save()
                break
            except (CloudantException, HTTPError) as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e

    def get_key(self, database_name, document_id, key):
        db = CloudantDatabase(self.client, database_name)
        retry = self.max_retries
        while retry > 0:
            try:
                doc = Document(db, document_id=document_id)
                doc.fetch()
                value = doc[key] if key in doc else None
                return value
            except (CloudantException, HTTPError) as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e
